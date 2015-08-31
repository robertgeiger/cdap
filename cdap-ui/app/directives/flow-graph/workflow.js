var module = angular.module(PKG.name+'.commons');

var tip;
var baseDirective = {
  restrict: 'E',
  templateUrl: 'flow-graph/flow.html',
  scope: {
    model: '=',
    onChangeFlag: '=',
    clickContext: '=',
    click: '&'
  },
  controller: 'myFlowController'
};


module.directive('myWorkflowGraph', function ($filter, $location, FlowFactories) {
  return angular.extend({
    link: function (scope) {
      scope.render = FlowFactories.genericRender.bind(null, scope, $filter, $location, tip);

      var defaultRadius = 50;
      scope.getShapes = function() {
        var shapes = {};
        shapes.job = function(parent, bbox, node) {

          // Creating Hexagon
          var xPoint = defaultRadius * 7/8;
          var yPoint = defaultRadius * 1/2;
          var points = [
            // points are listed from top and going clockwise
            { x: 0, y: defaultRadius},
            { x: xPoint, y: yPoint},
            { x: xPoint, y: -yPoint },
            { x: 0, y: -defaultRadius},
            { x: -xPoint, y: -yPoint},
            { x: -xPoint, y: yPoint}
          ];
          var shapeSvg = parent.insert('polygon', ':first-child')
              .attr('points', points.map(function(p) { return p.x + ',' + p.y; }).join(' '));
          var status = (scope.model.current && scope.model.current[node.elem.__data__]) || '';
          switch(status) {
            case 'COMPLETED':
              shapeSvg.attr('class', 'workflow-shapes foundation-shape job-svg completed');
              break;
            case 'RUNNING':
              shapeSvg.attr('class', 'workflow-shapes foundation-shape job-svg running');
              break;
            case 'FAILED':
              shapeSvg.attr('class', 'workflow-shapes foundation-shape job-svg failed');
              break;
            case 'KILLED':
              shapeSvg.attr('class', 'workflow-shapes foundation-shape job-svg killed');
              break;
            default:
              shapeSvg.attr('class', 'workflow-shapes foundation-shape job-svg');
          }

          node.intersect = function(point) {
            return dagreD3.intersect.polygon(node, points, point);
          };

          return shapeSvg;
        };

        shapes.start = function(parent, bbox, node) {
          var w = bbox.width;
          var points = [
            // draw a triangle facing right
            { x: -30, y: -40},
            { x: 30, y: 0},
            { x: -30, y: 40},
          ];
          var shapeSvg = parent.insert('polygon', ':first-child')
            .attr('points', points.map(function(p) { return p.x + ',' + p.y; }).join(' '))
            .attr('transform', 'translate(' + (w/6) + ')')
            .attr('class', 'workflow-shapes foundation-shape start-svg');

          node.intersect = function(point) {
            return dagreD3.intersect.polygon(node, points, point);
          };

          return shapeSvg;
        };

        shapes.end = function(parent, bbox, node) {
          var w = bbox.width;
          var points = [
            // draw a triangle facing right
            { x: -30, y: 0},
            { x: 30, y: 40},
            { x: 30, y: -40},
          ];
          var shapeSvg = parent.insert('polygon', ':first-child')
            .attr('points', points.map(function(p) { return p.x + ',' + p.y; }).join(' '))
            .attr('transform', 'translate(' + (-w/6) + ')')
            .attr('class', 'workflow-shapes foundation-shape end-svg');

          node.intersect = function(point) {
            return dagreD3.intersect.polygon(node, points, point);
          };

          return shapeSvg;
        };

        shapes.conditional = function(parent, bbox, node) {
          var points = [
            // draw a diamond
            { x:  0, y: -defaultRadius*3/4 },
            { x: -defaultRadius*3/4, y:  0 },
            { x:  0, y:  defaultRadius*3/4 },
            { x:  defaultRadius*3/4, y:  0 }
          ],
          shapeSvg = parent.insert('polygon', ':first-child')
            .attr('points', points.map(function(p) { return p.x + ',' + p.y; }).join(' '))
            .attr('class', 'workflow-shapes foundation-shape conditional-svg');

          node.intersect = function(p) {
            return dagreD3.intersect.polygon(node, points, p);
          };
          return shapeSvg;
        };

        shapes.forkjoin = function(parent, bbox, node) {

          parent.select('.label')
            .attr('style', 'display: none;');

          var shapeSvg = parent.insert('circle', ':first-child')
            .attr('x', -bbox.width / 2)
            .attr('y', -bbox.height / 2)
            .attr('r', 0);


          node.intersect = function(p) {
            return dagreD3.intersect.circle(node, 1, p);
          };

          return shapeSvg;
        };

        return shapes;
      };

      scope.getShape = function(name) {
        var shapeName;

        switch(name) {
          case 'ACTION':
            shapeName = 'job';
            break;
          case 'FORKNODE':
            shapeName = 'forkjoin';
            break;
          case 'JOINNODE':
            shapeName = 'forkjoin';
            break;
          case 'START':
            shapeName = 'start';
            break;
          case 'END':
            shapeName = 'end';
            break;
          default:
            shapeName = 'conditional';
            break;
        }
        return shapeName;
      };

      scope.handleNodeClick = function(nodeId) {

        scope.handleHideTip(nodeId);
        var instance = scope.instanceMap[nodeId];
        scope.$apply(function(scope) {
          var fn = scope.click();
          if ('undefined' !== typeof fn) {
            fn.call(scope.clickContext, instance);
          }
        });
      };

      scope.handleTooltip = function(tip, nodeId) {
        if (['Start', 'End'].indexOf(nodeId) === -1) {
          tip
            .html(function() {
              return '<span>'+ scope.instanceMap[nodeId].nodeId + ' : ' + scope.instanceMap[nodeId].program.programName +'</span>';
            })
            .show();
        }

      };

      scope.arrowheadRule = function(edge) {
        if (edge.targetType === 'JOINNODE' || edge.targetType === 'FORKNODE') {
          return false;
        } else {
          return true;
        }
      };
    }
  }, baseDirective);
});
