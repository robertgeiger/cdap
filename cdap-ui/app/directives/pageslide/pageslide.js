angular.module(PKG.name+ '.commons')
  .directive('myPageslide', function ($timeout) {
    return {
      restrict: 'E',
      transclude: false,
      scope: {
        open: '=',
        width: '@'
      },
      link: function pageslide(scope, elem) {
        scope.width = scope.width || '600';

        var slide = elem[0];

        slide.style.position = 'absolute';
        slide.style.overflowY = 'auto';
        slide.style.overflowX = 'hidden';
        slide.style.width = '0px';
        slide.style.right = '0px';

        slide.style.bottom = '52px'; // height of footer
        slide.style.backgroundColor = 'rgb(102, 110, 130)';
        slide.style.padding = '25px';
        slide.style.zIndex = 1000;
        slide.style.transitionDuration = '0.5s';
        slide.style.webkitTransitionDuration = '0.5s';
        slide.style.transitionProperty = 'width, height';

        // The calculation of the offset top needs to happen after the template is rendered
        $timeout(function () {
          var anchor = angular.element(document.getElementById('sidepanel-anchor'));
          var offset = anchor[0].offsetTop;
          slide.style.top = offset + 'px';
        });


        function open(slider) {
          slider.style.width = scope.width + 'px';
        }

        function close(slider) {
          slider.style.width = '0px';
          scope.open = false;
        }

        scope.$watch('open', function (value) {
          if (!!value) {
            open(slide);
          } else {
            close(slide);
          }
        });

        scope.$on('$destroy', function () {
          close(slide);
        });

        scope.$on('$locationChangeStart', function () {
          close(slide);
        });

        scope.$on('$stateChangeStart', function () {
          close(slide);
        });

      }
    };
  });


