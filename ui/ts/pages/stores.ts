// source: pages/stores.ts
/// <reference path="../../bower_components/mithriljs/mithril.d.ts" />
/// <reference path="../models/status.ts" />
/// <reference path="../components/metrics.ts" />
/// <reference path="../components/table.ts" />
/// <reference path="../components/navbar.ts" />
/// <reference path="../components/topbar.ts" />
/// <reference path="../components/visualizations/visualizations.ts" />
/// <reference path="../util/format.ts" />

// Author: Bram Gruneir (bram+code@cockroachlabs.com)

/**
 * AdminViews is the primary module for Cockroaches administrative web
 * interface.
 */
module AdminViews {
  "use strict";

  import MithrilElement = _mithril.MithrilVirtualElement;

  /**
   * Stores is the view for exploring the status of all Stores.
   */
  export module Stores {
    import Metrics = Models.Metrics;
    import Table = Components.Table;
    import StoreStatus = Models.Proto.StoreStatus;
    import Moment = moment.Moment;
    import MithrilVirtualElement = _mithril.MithrilVirtualElement;

    let storeStatuses: Models.Status.Stores = new Models.Status.Stores();

    function _storeMetric(metric: string): string {
      return "cr.store." + metric;
    }

    /**
     * StoresPage show a list of all the available nodes.
     */
    export module StoresPage {
      import Topbar = Components.Topbar;
      import MithrilComponent = _mithril.MithrilComponent;
      import bytesAndCountReducer = Models.Status.bytesAndCountReducer;
      import BytesAndCount = Models.Status.BytesAndCount;

      class Controller {
        private static _queryEveryMS: number = 10000;
        private static comparisonColumns: Table.TableColumn<StoreStatus>[] = [
          {
            title: "",
            view: (status: StoreStatus): MithrilVirtualElement => {
              let lastUpdate: Moment = moment(Utils.Convert.NanoToMilli(status.stats.last_update_nanos));
              let s: string = Models.Status.staleStatus(lastUpdate);
              return m("div.status.icon-cockroach-15." + s); // icon 15 is a circle
            },
            sortable: true,
          },
          {
            title: "Store ID",
            view: (status: StoreStatus): MithrilElement => {
              return m("a", {href: "/stores/" + status.desc.store_id, config: m.route}, status.desc.store_id.toString());
            },
            sortable: true,
            sortValue: (status: StoreStatus): number => status.desc.store_id,
            rollup: function(rows: StoreStatus[]): MithrilVirtualElement {
              interface StatusTotals {
                missing?: number;
                stale?: number;
                healthy?: number;
              }
              let statuses: StatusTotals = _.countBy(rows, (row: StoreStatus) => Models.Status.staleStatus(moment(Utils.Convert.NanoToMilli(row.stats.last_update_nanos))));

              return m("node-counts", [
                m("span.healthy", statuses.healthy || 0),
                m("span", "/"),
                m("span.stale", statuses.stale || 0),
                m("span", "/"),
                m("span.missing", statuses.missing || 0),
              ]);
            },
          },
          {
            title: "Node ID",
            view: (status: StoreStatus): MithrilElement => {
              return m("a", {href: "/nodes/" + status.desc.node.node_id, config: m.route}, status.desc.node.node_id.toString());
            },
            sortable: true,
            sortValue: (status: StoreStatus): number => status.desc.node.node_id,
          },
          {
            title: "Address",
            view: (status: StoreStatus): string => status.desc.node.address.address,
            sortable: true,
          },
          {
            title: "Started At",
            view: (status: StoreStatus): string => {
              let date = new Date(Utils.Convert.NanoToMilli(status.started_at));
              return Utils.Format.Date(date);
            },
            sortable: true,
          },
          {
            title: "Live Count (Bytes)",
            view: (status: StoreStatus): string =>
              status.stats.live_count + " (" + Utils.Format.Bytes(status.stats.live_bytes) + ")",
            sortable: true,
            sortValue: (status: StoreStatus): number => status.stats.live_bytes,
            rollup: function(rows: StoreStatus[]): string {
              let total: BytesAndCount = bytesAndCountReducer("stats.live_bytes", "stats.live_count", rows);
              return Utils.Format.Bytes(total.bytes) + " (" + total.count + ")";
            },
            section: "storage",
          },
          {
            title: "Key Count (Bytes)",
            view: (status: StoreStatus): string =>
              status.stats.key_count + " (" + Utils.Format.Bytes(status.stats.key_bytes) + ")",
            sortable: true,
            sortValue: (status: StoreStatus): number => status.stats.key_bytes,
            rollup: function(rows: StoreStatus[]): string {
              let total: BytesAndCount = bytesAndCountReducer("stats.key_bytes", "stats.key_count", rows);
              return Utils.Format.Bytes(total.bytes) + " (" + total.count + ")";
            },
            section: "storage",
          },
          {
            title: "Value Count (Bytes)",
            view: (status: StoreStatus): string =>
              status.stats.val_count + " (" + Utils.Format.Bytes(status.stats.val_bytes) + ")",
            sortable: true,
            sortValue: (status: StoreStatus): number => status.stats.val_bytes,
            rollup: function(rows: StoreStatus[]): string {
              let total: BytesAndCount = bytesAndCountReducer("stats.val_bytes", "stats.val_count", rows);
              return Utils.Format.Bytes(total.bytes) + " (" + total.count + ")";
            },
            section: "storage",
          },
          {
            title: "Intent Count (Bytes)",
            view: (status: StoreStatus): string =>
              status.stats.intent_count + " (" + Utils.Format.Bytes(status.stats.intent_bytes) + ")",
            sortable: true,
            sortValue: (status: StoreStatus): number => status.stats.intent_bytes,
            rollup: function(rows: StoreStatus[]): string {
              let total: BytesAndCount = bytesAndCountReducer("stats.intent_bytes", "stats.intent_count", rows);
              return Utils.Format.Bytes(total.bytes) + " (" + total.count + ")";
            },
            section: "storage",
          },
          {
            title: "System Count (Bytes)",
            view: (status: StoreStatus): string =>
              status.stats.sys_count + " (" + Utils.Format.Bytes(status.stats.sys_bytes) + ")",
            sortable: true,
            sortValue: (status: StoreStatus): number => status.stats.sys_bytes,
            rollup: function(rows: StoreStatus[]): string {
              let total: BytesAndCount = bytesAndCountReducer("stats.sys_bytes", "stats.sys_count", rows);
              return Utils.Format.Bytes(total.bytes) + " (" + total.count + ")";
            },
            section: "storage",
          },
        ];

        public sources: string[] = [];
        public columns: Utils.Property<Table.TableColumn<StoreStatus>[]> = Utils.Prop(Controller.comparisonColumns);
        exec: Metrics.Executor;
        axes: Metrics.Axis[] = [];
        private _query: Metrics.Query;
        private _interval: number;

        public constructor(nodeId?: string) {
          this._query = Metrics.NewQuery();
          this._addChart(
            Metrics.NewAxis(
              Metrics.Select.Avg(_storeMetric("keycount"))
                .sources(this.sources)
                .title("Key Count")
              )
              .label("Count")
            );
          this._addChart(
            Metrics.NewAxis(
              Metrics.Select.Avg(_storeMetric("livecount"))
                .sources(this.sources)
                .title("Live Value Count")
              )
              .label("Count")
            );
          this._addChart(
            Metrics.NewAxis(
              Metrics.Select.Avg(_storeMetric("valcount"))
                .sources(this.sources)
                .title("Total Value Count")
              )
              .label("Count")
            );
          this._addChart(
            Metrics.NewAxis(
              Metrics.Select.Avg(_storeMetric("intentcount"))
                .sources(this.sources)
                .title("Intent Count")
              )
              .label("Count")
            );
          this._addChart(
            Metrics.NewAxis(
              Metrics.Select.Avg(_storeMetric("ranges"))
                .sources(this.sources)
                .title("Range Count")
              )
              .label("Count")
            );
          this._addChart(
            Metrics.NewAxis(
              Metrics.Select.Avg(_storeMetric("livebytes"))
                .sources(this.sources)
                .title("Live Bytes")
              )
              .label("Bytes")
              .format(Utils.Format.Bytes)
            );

          this.exec = new Metrics.Executor(this._query);
          this._refresh();
          this._interval = window.setInterval(() => this._refresh(), Controller._queryEveryMS);
        }

        public onunload(): void {
          clearInterval(this._interval);
        }

        public RenderPrimaryStats(): MithrilElement {
          let allStats: Models.Proto.Status = storeStatuses.totalStatus();
          if (allStats) {
            return m(".primary-stats", [
                {
                  title: "Total Ranges",
                  visualizationArguments: {
                    format: ".0s",
                    data: {value: allStats.range_count},
                  },
                },
                {
                  title: "Total Live Bytes",
                  visualizationArguments: {
                    formatFn: function (v: number): string {
                      return Utils.Format.Bytes(v);
                    },
                    zoom: "50%",
                    data: {value: allStats.stats.live_bytes},
                  },
                },
                {
                  title: "Leader Ranges",
                  visualizationArguments: {
                    format: ".0s",
                    data: {value: allStats.leader_range_count},
                  },
                },
                {
                  title: "Available",
                  visualizationArguments: {
                    format: "3%",
                    data: {value: allStats.available_range_count / allStats.leader_range_count},
                  },
                },
                {
                  title: "Fully Replicated",
                  visualizationArguments: {
                    format: "3%",
                    data: {value: allStats.replicated_range_count / allStats.leader_range_count},
                  },
                },
              ].map(function (v: any): MithrilComponent<any> {
                v.virtualVisualizationElement =
                  m.component(Visualizations.NumberVisualization, v.visualizationArguments);
                return m.component(Visualizations.VisualizationWrapper, v);
              })
            );
          }
          return m(".primary-stats");
        }

        public RenderGraphs(): MithrilElement {
          return m(".charts", this.axes.map((axis: Metrics.Axis) => {
            return m("", { style: "float:left" }, Components.Metrics.LineGraph.create(this.exec, axis));
          }));
        }

        private _refresh(): void {
          this.exec.refresh();
          storeStatuses.refresh();
        }

        private _addChart(axis: Metrics.Axis): void {
          axis.selectors().forEach((s: Metrics.Select.Selector) => this._query.selectors().push(s));
          this.axes.push(axis);
        }
      }

      export function controller(): Controller {
        return new Controller();
      }

      export function view(ctrl: Controller): MithrilElement {
        ctrl.sources = _.map(
          storeStatuses.allStatuses(),
          function(v: StoreStatus): string {
            return v.desc.node.node_id.toString();
          }
        );
        let comparisonData: Table.TableData<StoreStatus> = {
          columns: ctrl.columns,
          rows: storeStatuses.allStatuses,
        };

        let mostRecentlyUpdated: number = _.max(_.map(storeStatuses.allStatuses(), (s: StoreStatus) => s.updated_at ));
        return m(".page", [
          m.component(Topbar, {title: "Stores", updated: mostRecentlyUpdated }),
          m(".section", ctrl.RenderGraphs()),
          m(".section.table", m(".stats-table", Components.Table.create(comparisonData))),
        ]);
      }
    }

    /**
     * StorePage show the details of a single node.
     */
    export module StorePage {
      import NavigationBar = Components.NavigationBar;

      class Controller {
        private static defaultTargets: NavigationBar.Target[] = [
          {
            view: "Overview",
            route: "",
          },
          {
            view: "Graphs",
            route: "graph",
          },
        ];

        private static isActive: (targ: NavigationBar.Target) => boolean = (t: NavigationBar.Target) => {
          return ((m.route.param("detail") || "") === t.route);
        };

        private static _queryEveryMS: number = 10000;
        exec: Metrics.Executor;
        axes: Metrics.Axis[] = [];
        private _query: Metrics.Query;
        private _interval: number;
        private _storeId: string;

        public onunload(): void {
          clearInterval(this._interval);
        }

        public constructor(storeId: string) {
          this._storeId = storeId;
          this._query = Metrics.NewQuery();
          this._addChart(
            Metrics.NewAxis(
              Metrics.Select.Avg(_storeMetric("keycount"))
                .sources([storeId])
                .title("Key Count")
              )
              .label("Count")
            );
          this._addChart(
            Metrics.NewAxis(
              Metrics.Select.Avg(_storeMetric("livecount"))
                .sources([storeId])
                .title("Live Value Count")
              )
              .label("Count")
            );
          this._addChart(
            Metrics.NewAxis(
              Metrics.Select.Avg(_storeMetric("valcount"))
                .sources([storeId])
                .title("Total Value Count")
              )
              .label("Count")
            );
          this._addChart(
            Metrics.NewAxis(
              Metrics.Select.Avg(_storeMetric("intentcount"))
                .sources([storeId])
                .title("Intent Count")
              )
              .label("Count")
            );
          this._addChart(
            Metrics.NewAxis(
              Metrics.Select.Avg(_storeMetric("ranges"))
                .sources([storeId])
                .title("Range Count")
              )
              .label("Count")
            );
          this._addChart(
            Metrics.NewAxis(
              Metrics.Select.Avg(_storeMetric("livebytes"))
                .sources([storeId])
                .title("Live Bytes")
              )
              .label("Bytes")
              .format(Utils.Format.Bytes)
            );

          this.exec = new Metrics.Executor(this._query);
          this._refresh();
          this._interval = window.setInterval(() => this._refresh(), Controller._queryEveryMS);
        }

        public RenderPrimaryStats(): MithrilElement {
          let storeStats: Models.Proto.StoreStatus = storeStatuses.GetStatus(this._storeId);
          if (storeStats) {
            return m(".primary-stats", [
              m(".stat", [
                m("span.title", "Started At"),
                m("span.value", Utils.Format.Date(new Date(Utils.Convert.NanoToMilli(storeStats.started_at)))),
              ]),
              m(".stat", [
                m("span.title", "Last Updated At"),
                m("span.value", Utils.Format.Date(new Date(Utils.Convert.NanoToMilli(storeStats.updated_at)))),
              ]),
              m(".stat", [
                m("span.title", "Total Ranges"),
                m("span.value", storeStats.range_count),
              ]),
              m(".stat", [
                m("span.title", "Total Live Bytes"),
                m("span.value", Utils.Format.Bytes(storeStats.stats.live_bytes)),
              ]),
              m(".stat", [
                m("span.title", "Leader Ranges"),
                m("span.value", storeStats.leader_range_count),
              ]),
              m(".stat", [
                m("span.title", "Available"),
                m("span.value", Utils.Format.Percentage(storeStats.available_range_count, storeStats.leader_range_count)),
              ]),
              m(".stat", [
                m("span.title", "Fully Replicated"),
                m("span.value", Utils.Format.Percentage(storeStats.replicated_range_count, storeStats.leader_range_count)),
              ]),
            ]);
          }
          return m(".primary-stats");
        }

        public RenderGraphs(): MithrilElement {
          return m(".charts", this.axes.map((axis: Metrics.Axis) => {
            return m("", { style: "float:left" }, [
              m("h4", axis.title()),
              Components.Metrics.LineGraph.create(this.exec, axis),
            ]);
          }));
        }

        public TargetSet(): NavigationBar.TargetSet {
          return {
            baseRoute: "/stores/" + this._storeId + "/",
            targets: Utils.Prop(Controller.defaultTargets),
            isActive: Controller.isActive,
          };
        }

        public GetStoreId(): string {
          return this._storeId;
        }

        private _refresh(): void {
          storeStatuses.refresh();
          this.exec.refresh();
        }

        private _addChart(axis: Metrics.Axis): void {
          axis.selectors().forEach((s: Metrics.Select.Selector) => this._query.selectors().push(s));
          this.axes.push(axis);
        }
      }

      export function controller(): Controller {
        let storeId: string = m.route.param("store_id");
        return new Controller(storeId);
      }

      export function view(ctrl: Controller): MithrilElement {
        let detail: string = m.route.param("detail");

        // Page title.
        let title: string = "Stores: Store " + ctrl.GetStoreId();
        if (detail === "graph") {
          title += ": Graphs";
        }

        // Primary content
        let primaryContent: MithrilElement;
        if (detail === "graph") {
          primaryContent = ctrl.RenderGraphs();
        } else {
          primaryContent = ctrl.RenderPrimaryStats();
        }

        let storeStatus: StoreStatus = storeStatuses.GetStatus(ctrl.GetStoreId());
        let updated: number = (storeStatus ? storeStatus.updated_at : 0);

        return m(".page", [
          m.component(Components.Topbar, {title: title, updated: updated}),
          m.component(NavigationBar, {ts: ctrl.TargetSet()}),
          m(".section", primaryContent),
        ]);
      }
    }
  }
}
