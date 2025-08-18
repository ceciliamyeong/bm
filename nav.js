<!-- bm20/nav.js -->
<script>
function loadBmNav(activeKey){
  fetch('/bm20/nav.html', {cache:'no-store'})
    .then(r=>r.text())
    .then(html=>{
      const wrap=document.getElementById('bm-nav');
      wrap.innerHTML=html;
      wrap.querySelectorAll('a[data-key]').forEach(a=>{
        if(a.dataset.key===activeKey) a.classList.add('on');
      });
    })
    .catch(err=>console.error('nav load failed', err));
}
</script>
