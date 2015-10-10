/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

angular.module(PKG.name + '.commons')
  .factory('MyDAGFactory', function(CanvasFactory) {
    var defaultSettings = {
      Connector : [ 'Flowchart', {gap: 7} ],
      ConnectionsDetachable: true
    };
    var connectorStyle = {
      strokeStyle: '#666e82',
      fillStyle: '#666e82',
      radius: 5,
      lineWidth: 2
    };
    function createSchemaOnEdge() {
      return angular.element('<div><div class="label-container text-center"><i class="icon-SchemaEdge"></i></div></div>');
    }
    var connectorOverlays = {
      connectorOverlays: [
        [ 'Arrow', { location: 1, length: 12, width: 12, height: 10, foldback: 1 } ],
        [ 'Custom', {
          create: createSchemaOnEdge,
          location: 0.5,
          id: 'label'
        }]
      ]
    };
    var disabledConnectorOverlays = {
      connectorOverlays: [
        [ 'Arrow', { location: 1, length: 12, width: 12, height: 10, foldback: 1 } ]
      ]
    };

    var commonSettings = {
      endpoint:'Dot',
      maxConnections: -1, // -1 means unlimited connections
      paintStyle: {
        strokeStyle: '#666e82',
        fillStyle: '#666e82',
        radius: 5,
        lineWidth: 3
      },
      anchors: [ 'Static']
    };
    var sourceSettings = angular.extend({
      isSource: true,
      connectorStyle: connectorStyle,
      anchor: [ 0.5, 1, 1, 0, 26, -43, 'sourceAnchor']
    }, commonSettings);
    var sinkSettings = angular.extend({
      isTarget: true,
      anchor: [ 0.5, 1, -1, 0, -26, -43, 'sinkAnchor'],
      connectorStyle: connectorStyle
    }, commonSettings);

    function getSettings(isDisabled) {
      var settings = {};
      if (isDisabled) {
        settings = {
          default: defaultSettings,
          commonSettings: angular.extend(commonSettings, disabledConnectorOverlays),
          source: angular.extend(sourceSettings, disabledConnectorOverlays),
          sink: angular.extend(sinkSettings, disabledConnectorOverlays)
        };
      } else {
        settings = {
          default: defaultSettings,
          commonSettings: angular.extend(commonSettings, connectorOverlays),
          source: angular.extend(sourceSettings, connectorOverlays),
          sink: angular.extend(sinkSettings, connectorOverlays)
        };
      }
      return settings;
    }

    function getIcon(plugin) {
      var iconMap = {
        'script': 'fa-code',
        'scriptfilter': 'fa-code',
        'twitter': 'fa-twitter',
        'cube': 'fa-cubes',
        'data': 'fa-database',
        'database': 'fa-database',
        'table': 'fa-table',
        'kafka': 'icon-kafka',
        'stream': 'icon-plugin-stream',
        'tpfsavro': 'icon-avro',
        'jms': 'icon-jms',
        'projection': 'icon-projection'
      };

      var pluginName = plugin.toLowerCase();
      var icon = iconMap[pluginName] ? iconMap[pluginName]: 'fa-plug';
      return icon;
    }

    function generateStyles(name, nodes, xmargin, ymargin) {
      var styles = {};
      var nodeStylesFromDagre = nodes.filter(function(node) {
        return node.label === name;
      });

      if (nodeStylesFromDagre.length) {
        styles = {
          'top': (nodeStylesFromDagre[0].y + ymargin) + 'px',
          'left': (nodeStylesFromDagre[0].x + xmargin) + 'px'
        };
      }
      return styles;
    }

    // Using Dagre here to generate x and y co-ordinates for each node.
    // When we fork and branch and have complex connections this will be useful for us.
    // Right now this returns a pretty simple straight linear graph.
     function getGraph(plugins, type) {
      var graph = new dagre.graphlib.Graph();
      graph.setGraph({
        nodesep: 90,
        ranksep: 100,
        rankdir: 'LR',
        marginx: 30,
        marginy: 30
      });

      graph.setDefaultEdgeLabel(function() { return {}; });
      plugins.forEach(function(plugin) {
        graph.setNode(plugin.id, {label: plugin.id, width: 100, height: 100});
      });

      var connections = CanvasFactory.getConnectionsBasedOnNodes(plugins, type);
      connections.forEach(function (connection) {
        graph.setEdge(connection.source, connection.target);
      });

      dagre.layout(graph);
      return graph;
    }

    return {
      getSettings: getSettings,
      getIcon: getIcon,
      generateStyles: generateStyles,
      getGraph: getGraph
    };

  });
