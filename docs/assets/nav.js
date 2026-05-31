/* Databricks-native MDM blueprint — site enhancements (vanilla JS, no deps).
   Loaded by every page. Progressive: if anything is missing it degrades quietly. */
(function () {
  "use strict";
  var doc = document;

  /* ---------- 0. favicon (inline SVG) ---------- */
  if (!doc.querySelector("link[rel='icon']")) {
    var svg = "data:image/svg+xml," + encodeURIComponent(
      "<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'>" +
      "<rect width='32' height='32' rx='7' fill='#0d1117'/>" +
      "<circle cx='16' cy='16' r='7' fill='#ff5a3c'/>" +
      "<circle cx='16' cy='16' r='3' fill='#0d1117'/></svg>");
    var link = doc.createElement("link");
    link.rel = "icon"; link.href = svg; doc.head.appendChild(link);
  }

  /* ---------- 1. theme toggle (localStorage) ---------- */
  var root = doc.documentElement;
  var saved = null;
  try { saved = localStorage.getItem("mdm-theme"); } catch (e) {}
  if (saved) root.setAttribute("data-theme", saved);

  var topbar = doc.querySelector(".topbar .spacer");
  if (topbar) {
    var btn = doc.createElement("button");
    btn.className = "theme-toggle";
    btn.setAttribute("aria-label", "Toggle light/dark theme");
    var setIcon = function () {
      btn.textContent = root.getAttribute("data-theme") === "light" ? "🌙" : "☀";
    };
    setIcon();
    btn.addEventListener("click", function () {
      var next = root.getAttribute("data-theme") === "light" ? "dark" : "light";
      root.setAttribute("data-theme", next);
      try { localStorage.setItem("mdm-theme", next); } catch (e) {}
      setIcon();
    });
    topbar.parentNode.insertBefore(btn, topbar.nextSibling);
  }

  /* ---------- 2. inject "I · The Engine" nav link if absent ---------- */
  var sb = doc.querySelector(".sidebar");
  if (sb && !sb.querySelector("a[href='code.html']")) {
    var a = doc.createElement("a");
    a.href = "code.html";
    a.innerHTML = "<span class='lt'>I</span> The Engine (code)";
    sb.appendChild(a);
  }

  /* ---------- 3. active sidebar link ---------- */
  var here = location.pathname.split("/").pop() || "index.html";
  doc.querySelectorAll(".sidebar a").forEach(function (a) {
    var href = a.getAttribute("href");
    if (href === here || (here === "" && href === "index.html"))
      a.classList.add("active");
  });

  /* ---------- 4. mobile sidebar toggle ---------- */
  var tgl = doc.querySelector(".nav-toggle");
  if (tgl && sb) {
    tgl.addEventListener("click", function () { sb.classList.toggle("open"); });
    sb.addEventListener("click", function (e) {
      if (e.target.tagName === "A") sb.classList.remove("open");
    });
  }

  /* ---------- 5. code blocks: copy button + light syntax highlight ---------- */
  var BOX = /[─-╿■-◿•]/;       // box-drawing / bullets -> diagram
  var KW = /\b(?:def|class|return|import|from|if|else|elif|for|while|in|not|and|or|is|None|True|False|with|as|yield|raise|try|except|finally|lambda|self|async|await|pass|SELECT|FROM|WHERE|CREATE|TABLE|SCHEMA|CATALOG|MERGE|ALTER|SET|INSERT|UPDATE|GROUP|ORDER|BY)\b/;

  function esc(s) {
    return s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
  }
  function highlight(text) {
    var re = /(#[^\n]*|\/\/[^\n]*)|("[^"]*"|'[^']*')|(\b\d+\.?\d*\b)|([A-Za-z_]\w*(?=\())|([A-Za-z_]+)/g;
    return text.replace(re, function (m, com, str, num, fn, word) {
      if (com) return "<span class='tok-com'>" + esc(com) + "</span>";
      if (str) return "<span class='tok-str'>" + esc(str) + "</span>";
      if (num) return "<span class='tok-num'>" + num + "</span>";
      if (fn) return "<span class='tok-fn'>" + fn + "</span>";
      if (word && KW.test(word)) return "<span class='tok-kw'>" + word + "</span>";
      return esc(m);
    });
  }

  doc.querySelectorAll(".content pre").forEach(function (pre) {
    var wrap = doc.createElement("div");
    wrap.className = "code-wrap";
    pre.parentNode.insertBefore(wrap, pre);
    wrap.appendChild(pre);

    var raw = pre.textContent;
    if (!BOX.test(raw)) {                      // skip ASCII diagrams / trees
      pre.innerHTML = highlight(raw);
    }

    var cb = doc.createElement("button");
    cb.className = "copy-btn"; cb.textContent = "copy";
    cb.addEventListener("click", function () {
      navigator.clipboard.writeText(raw).then(function () {
        cb.textContent = "copied"; cb.classList.add("done");
        setTimeout(function () { cb.textContent = "copy"; cb.classList.remove("done"); }, 1400);
      });
    });
    wrap.appendChild(cb);
  });

  /* ---------- 6. heading anchors + on-this-page TOC ---------- */
  var content = doc.querySelector(".content");
  var heads = content ? content.querySelectorAll("h2, h3") : [];
  if (heads.length > 2) {
    var toc = doc.createElement("nav");
    toc.className = "on-this-page";
    toc.innerHTML = "<div class='t'>On this page</div>";
    var slugged = {};
    heads.forEach(function (h) {
      var slug = (h.textContent || "").toLowerCase().trim()
        .replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "").slice(0, 48) || "s";
      if (slugged[slug]) slug += "-" + (++slugged[slug]); else slugged[slug] = 1;
      h.id = slug;
      var al = doc.createElement("a");
      al.className = "anchor-link"; al.href = "#" + slug; al.textContent = "#";
      h.appendChild(al);
      var t = doc.createElement("a");
      t.href = "#" + slug;
      t.className = h.tagName === "H3" ? "h3" : "h2";
      t.textContent = (h.textContent || "").replace(/#$/, "").trim();
      toc.appendChild(t);
    });
    doc.body.appendChild(toc);

    var links = toc.querySelectorAll("a");
    var spy = function () {
      var pos = window.scrollY + 120, cur = null;
      heads.forEach(function (h) { if (h.offsetTop <= pos) cur = h.id; });
      links.forEach(function (l) {
        l.classList.toggle("active", l.getAttribute("href") === "#" + cur);
      });
    };
    window.addEventListener("scroll", spy, { passive: true });
    spy();
  }

  /* ---------- 7. back to top ---------- */
  var top = doc.createElement("button");
  top.className = "to-top"; top.setAttribute("aria-label", "Back to top");
  top.innerHTML = "↑";
  top.addEventListener("click", function () {
    window.scrollTo({ top: 0, behavior: "smooth" });
  });
  doc.body.appendChild(top);
  window.addEventListener("scroll", function () {
    top.classList.toggle("show", window.scrollY > 600);
  }, { passive: true });

  /* ---------- 8. footer ---------- */
  if (!doc.querySelector(".site-footer")) {
    var f = doc.createElement("footer");
    f.className = "site-footer";
    f.innerHTML =
      "<span>Databricks-Native MDM — architecture blueprint &amp; engine.</span>" +
      "<span><a href='index.html'>Home</a> · " +
      "<a href='code.html'>The Engine</a> · " +
      "<a href='https://github.com/aviral-bhardwaj/databricksmdm'>GitHub ↗</a></span>";
    doc.body.appendChild(f);
  }
})();
