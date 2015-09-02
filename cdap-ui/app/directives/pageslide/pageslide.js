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

        slide.style.bottom = '58px'; // height of footer
        slide.style.backgroundColor = '#F6F6F6';
        slide.style.padding = '0';
        slide.style.zIndex = 1000;
        slide.style.transitionDuration = '0.5s';
        slide.style.webkitTransitionDuration = '0.5s';
        slide.style.transitionProperty = 'width, height';
        slide.style.boxShadow = '-3px 0px 5px 0px rgba(0,0,0,0.25)';

        // The calculation of the offset top needs to happen after the template is rendered
        $timeout(function () {
          var anchor = angular.element(document.getElementById('sidepanel-anchor'));
          var offset = anchor[0].offsetTop + 2;
          slide.style.top = offset + 'px';
        });


        function open(slider) {
          slider.style.width = scope.width + 'px';
          slide.style.backgroundColor = '#F6F6F6';
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


