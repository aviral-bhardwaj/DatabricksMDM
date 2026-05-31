/* Minimal nav behaviour: mobile toggle + active-link highlight. No framework. */
(function () {
  // Highlight the sidebar link matching the current page.
  var here = location.pathname.split('/').pop() || 'index.html';
  document.querySelectorAll('.sidebar a').forEach(function (a) {
    var href = a.getAttribute('href');
    if (href === here || (here === '' && href === 'index.html')) {
      a.classList.add('active');
    }
  });

  // Mobile sidebar toggle.
  var btn = document.querySelector('.nav-toggle');
  var sb = document.querySelector('.sidebar');
  if (btn && sb) {
    btn.addEventListener('click', function () { sb.classList.toggle('open'); });
    sb.addEventListener('click', function (e) {
      if (e.target.tagName === 'A') sb.classList.remove('open');
    });
  }
})();
