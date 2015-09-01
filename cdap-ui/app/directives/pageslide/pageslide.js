angular.module(PKG.name+ '.commons')
  .directive('myPageslide', function () {
    return {
      restrict: 'E',
      transclude: false,
      scope: {
        location: '@',
        open: '=',
        container: '@'
      },
      link: pageslide
    };
  });

function pageslide(scope, elem) {
  var parent = angular.element(elem[0].parentElement);
  console.log('parent', parent);
  var slide = elem[0];

  slide.style.position = 'absolute';
  slide.style.overflowY = 'auto';
  slide.style.overflowX = 'hidden';
  slide.style.width = '0px';
  slide.style.maxHeight = parent[0].offsetTop + 'px';
  slide.style.right = '0px';
  slide.style.top = parent[0].offsetTop + 'px';
  slide.style.backgroundColor = 'rgb(102, 110, 130)';
  slide.style.padding = '25px';
  slide.style.zIndex = 1000;
  slide.style.transitionDuration = '0.5s';
  slide.style.webkitTransitionDuration = '0.5s';
  slide.style.transitionProperty = 'width, height';


  function open(slider) {
    slider.style.width = '600px';
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