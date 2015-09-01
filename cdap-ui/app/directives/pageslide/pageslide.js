angular.module(PKG.name+ '.commons')
  .directive('myPageslide', function () {
    return {
      restrict: 'E',
      transclude: false,
      scope: {
        open: '=',
        width: '@'
      },
      link: pageslide
    };
  });

function pageslide(scope, elem) {
  scope.width = scope.width || '600';

  // var parent = angular.element(elem[0].parentElement);
  var slide = elem[0];

  var anchor = angular.element(document.getElementById('sidepanel-anchor'));
  var offset = anchor[0].offsetTop;

  slide.style.position = 'absolute';
  slide.style.overflowY = 'auto';
  slide.style.overflowX = 'hidden';
  slide.style.width = '0px';
  slide.style.right = '0px';
  slide.style.top = offset + 'px';
  slide.style.bottom = '52px'; // height of footer
  slide.style.backgroundColor = 'rgb(102, 110, 130)';
  slide.style.padding = '25px';
  slide.style.zIndex = 1000;
  slide.style.transitionDuration = '0.5s';
  slide.style.webkitTransitionDuration = '0.5s';
  slide.style.transitionProperty = 'width, height';


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
