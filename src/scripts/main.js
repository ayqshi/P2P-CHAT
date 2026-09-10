
(()=>{'use strict';
const $=s=>document.querySelector(s);
const uid=()=>crypto.randomUUID?.()??'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g,c=>(c==='x'?Math.random()*16|0:(Math.random()*16|0&0x3|0x8)).toString(16));
const sid=i=>{const c=i.replace(/-/g,'');return c.slice(0,4)+':'+c.slice(4,8)};
const now=()=>Date.now();
const san=s=>{const d=document.createElement('div');d.textContent=s??'';return d.innerHTML};
const tf=t=>new Date(t).toLocaleTimeString([],{hour:'numeric',minute:'2-digit'});
const df=t=>{const d=new Date(t),n=new Date;if(d.toDateString()===n.toDateString())return'Today';const y=new Date(n);y.setDate(y.getDate()-1);if(d.toDateString()===y.toDateString())return'Yesterday';return d.toLocaleDateString([],{month:'short',day:'numeric'})};
const rt=t=>{const s=(now()-t)/1e3|0;if(s<60)return'now';if(s<3600)return(s/60|0)+'m';if(s<86400)return(s/3600|0)+'h';return(s/86400|0)+'d'};
const fz=b=>b<1024?b+'B':b<1048576?(b/1024).toFixed(1)+'KB':(b/1048576).toFixed(1)+'MB';
const lnk=s=>s.replace(/(https?:\/\/[^\s<]+)/g,'<a href="$1" target="_blank" rel="noopener">$1</a>');
const vib=p=>{try{if(S.cfg.vib)navigator.vibrate?.(p)}catch(e){}};
const ag=el=>{el.style.height='auto';el.style.height=Math.min(el.scrollHeight,88)+'px'};
const scr=el=>{if(el)requestAnimationFrame(()=>el.scrollTop=el.scrollHeight)};
const toHttps=u=>typeof u==='string'?u.replace(/^http:\/\//,'https://'):u;

const PERSON_SVG='<svg class="defav" viewBox="0 0 24 24" fill="none"><circle cx="12" cy="8.4" r="3.6" stroke="currentColor" stroke-width="1.6"/><path d="M4.5 19.6c1-3.7 4.1-5.8 7.5-5.8s6.5 2.1 7.5 5.8" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"/></svg>';
const AV=url=>url?('<img src="'+url+'" loading="lazy" alt="">'):PERSON_SVG;

let S={me:null,fr:[],co:{},bl:[],no:{my:null,pe:{}},rq:[],nb:[],cfg:{rcpt:1,snd:1,vib:1,la:{}},bots:null,feed:[],netSeen:[]};
const LK='mj4_';
const load=()=>{try{Object.keys(S).forEach(k=>{const v=localStorage.getItem(LK+k);if(v!=null){const p=JSON.parse(v);
if(k==='cfg')S[k]={...S[k],...p};
else if(k==='no')S[k]={my:p?.my??null,pe:p?.pe??{}};
else S[k]=p}})}catch(e){}
if(!Array.isArray(S.feed))S.feed=[];if(!Array.isArray(S.netSeen))S.netSeen=[]};
const save=()=>{try{Object.keys(S).forEach(k=>localStorage.setItem(LK+k,JSON.stringify(S[k])))}catch(e){}};

let ac;
const chime=(kind)=>{if(!S.cfg.snd)return;try{const c=ac||(ac=new(AudioContext||webkitAudioContext));if(c.state==='suspended')c.resume();const t=c.currentTime;const g=c.createGain();g.gain.setValueAtTime(kind==='sent'?.12:.2,t);g.connect(c.destination);
const notes=kind==='sent'?[[880,1,.06],[1318.5,.4,.09]]:[[1174.66,1,.12],[2349.32,.25,.07],[587.33,.16,.16]];
notes.forEach(([f,a,d])=>{const o=c.createOscillator(),og=c.createGain();o.type='sine';o.frequency.value=f;og.gain.setValueAtTime(a,t);og.gain.exponentialRampToValueAtTime(.001,t+d);o.connect(og);og.connect(g);o.start(t);o.stop(t+d+.01)})}catch(e){}};

// ===== WebRTC Mesh & Direct Device-to-Device Transport Layer =====
const RTC_ICE_SERVERS = [
  { urls: ['stun:stun.l.google.com:19302', 'stun:stun1.l.google.com:19302', 'stun:stun2.l.google.com:19302'] },
  { urls: ['stun:stun.cloudflare.com:3478'] }
];
const _rtcPeers = {};
let _sigWs = null, _sigTimer = null;
const _tEnc = new TextEncoder(), _tDec = new TextDecoder();

function mqttEncode(topic, payloadStr) {
  const top = _tEnc.encode(topic), pay = _tEnc.encode(payloadStr);
  let v = 2 + top.length + pay.length, rem = [];
  do { let d = v % 128; v = v >> 7; if (v > 0) d |= 0x80; rem.push(d); } while (v > 0);
  const buf = new Uint8Array(1 + rem.length + 2 + top.length + pay.length);
  buf[0] = 0x30; buf.set(rem, 1);
  let o = 1 + rem.length;
  buf[o] = (top.length >> 8) & 0xff; buf[o + 1] = top.length & 0xff; o += 2;
  buf.set(top, o); o += top.length;
  buf.set(pay, o);
  return buf;
}

function initSig() {
  if (_sigWs && (_sigWs.readyState === WebSocket.OPEN || _sigWs.readyState === WebSocket.CONNECTING)) return;
  const brokers = ['wss://broker.emqx.io:8084/mqtt', 'wss://broker.hivemq.com:8884/mqtt'];
  const url = brokers[Math.floor(Math.random() * brokers.length)];
  try {
    _sigWs = new WebSocket(url, ['mqtt']);
    _sigWs.binaryType = 'arraybuffer';
    _sigWs.onopen = () => {
      const cid = 'mj_' + (S.me?.id || uid()).replace(/-/g, '').slice(0, 16);
      const cBytes = _tEnc.encode(cid);
      const proto = [0x00, 0x04, 0x4D, 0x51, 0x54, 0x54, 0x04, 0x02, 0x00, 0x3C];
      const buf = new Uint8Array(2 + proto.length + 2 + cBytes.length);
      buf[0] = 0x10; buf[1] = proto.length + 2 + cBytes.length;
      buf.set(proto, 2);
      buf[2 + proto.length] = (cBytes.length >> 8) & 0xff;
      buf[3 + proto.length] = cBytes.length & 0xff;
      buf.set(cBytes, 4 + proto.length);
      _sigWs.send(buf);

      const subTopic = (top) => {
        const tb = _tEnc.encode(top);
        const s = new Uint8Array(7 + tb.length);
        s[0] = 0x82; s[1] = 5 + tb.length; s[2] = 0x00; s[3] = 0x01;
        s[4] = (tb.length >> 8) & 0xff; s[5] = tb.length & 0xff;
        s.set(tb, 6); s[6 + tb.length] = 0x00;
        _sigWs.send(s);
      };
      subTopic('muji4/pub');
      if (S.me?.id) subTopic('muji4/p/' + S.me.id);
      clearInterval(_sigTimer);
      _sigTimer = setInterval(() => { if (_sigWs?.readyState === WebSocket.OPEN) _sigWs.send(new Uint8Array([0xC0, 0x00])); }, 30000);
      ann();
    };
    _sigWs.onmessage = e => {
      const data = new Uint8Array(e.data);
      if ((data[0] >> 4) === 3) {
        let pos = 1, mult = 1, rem = 0, digit;
        do { digit = data[pos++]; rem += (digit & 127) * mult; mult *= 128; } while ((digit & 128) !== 0);
        const tLen = (data[pos] << 8) | data[pos + 1]; pos += 2;
        pos += tLen;
        try {
          const payload = JSON.parse(_tDec.decode(data.subarray(pos)));
          onSig(payload);
        } catch (err) {}
      }
    };
    _sigWs.onclose = () => { clearInterval(_sigTimer); setTimeout(initSig, 3500); };
    _sigWs.onerror = () => { try { _sigWs.close(); } catch(e){} };
  } catch (e) {}
}

function sigTx(d) {
  if (!_sigWs || _sigWs.readyState !== WebSocket.OPEN) return;
  const topic = (d.to && d.to !== 'public') ? ('muji4/p/' + d.to) : 'muji4/pub';
  try { _sigWs.send(mqttEncode(topic, JSON.stringify(d))); } catch(e){}
}

function getOrCreatePeer(peerId, isInitiator) {
  if (_rtcPeers[peerId]) return _rtcPeers[peerId];
  const pc = new RTCPeerConnection({ iceServers: RTC_ICE_SERVERS });
  const entry = { id: peerId, pc, dc: null, pendingIce: [], inChunks: {} };
  _rtcPeers[peerId] = entry;

  pc.onicecandidate = e => {
    if (e.candidate) sigTx({ t: '_rtc_sig', from: S.me.id, to: peerId, kind: 'candidate', val: e.candidate });
  };
  pc.onconnectionstatechange = () => {
    if (pc.connectionState === 'disconnected' || pc.connectionState === 'failed' || pc.connectionState === 'closed') {
      try { pc.close(); } catch(e){}
      delete _rtcPeers[peerId];
    }
  };
  if (isInitiator) {
    const dc = pc.createDataChannel('muji_dc', { ordered: true });
    setupDC(peerId, dc);
    pc.createOffer().then(off => pc.setLocalDescription(off)).then(() => {
      sigTx({ t: '_rtc_sig', from: S.me.id, to: peerId, kind: 'offer', val: pc.localDescription });
    }).catch(()=>{});
  } else {
    pc.ondatachannel = e => setupDC(peerId, e.channel);
  }
  return entry;
}

function setupDC(peerId, dc) {
  if (_rtcPeers[peerId]) _rtcPeers[peerId].dc = dc;
  dc.onmessage = e => {
    try {
      const pkt = JSON.parse(e.data);
      if (pkt.__c === 0) onSig(pkt.d);
      else if (pkt.__c === 1) {
        const peer = _rtcPeers[peerId];
        if (!peer) return;
        if (!peer.inChunks[pkt.id]) peer.inChunks[pkt.id] = new Array(pkt.total);
        peer.inChunks[pkt.id][pkt.i] = pkt.chunk;
        if (peer.inChunks[pkt.id].filter(Boolean).length === pkt.total) {
          const full = peer.inChunks[pkt.id].join('');
          delete peer.inChunks[pkt.id];
          onSig(JSON.parse(full));
        }
      }
    } catch(err) {}
  };
  dc.onclose = () => { if (_rtcPeers[peerId]) _rtcPeers[peerId].dc = null; };
}

function rtcSend(peerId, obj) {
  const p = _rtcPeers[peerId];
  if (!p || !p.dc || p.dc.readyState !== 'open') return false;
  const raw = JSON.stringify(obj);
  if (raw.length < 28000) {
    p.dc.send(JSON.stringify({ __c: 0, d: obj }));
  } else {
    const mid = uid(), size = 28000, total = Math.ceil(raw.length / size);
    for (let i = 0; i < total; i++) {
      p.dc.send(JSON.stringify({ __c: 1, id: mid, i, total, chunk: raw.slice(i * size, (i + 1) * size) }));
    }
  }
  return true;
}

async function handleRtcSignal(d) {
  if (!S.me || d.to !== S.me.id) return;
  if (d.kind === 'offer') {
    const p = getOrCreatePeer(d.from, false);
    await p.pc.setRemoteDescription(new RTCSessionDescription(d.val));
    while (p.pendingIce.length) await p.pc.addIceCandidate(new RTCIceCandidate(p.pendingIce.shift()));
    const ans = await p.pc.createAnswer();
    await p.pc.setLocalDescription(ans);
    sigTx({ t: '_rtc_sig', from: S.me.id, to: d.from, kind: 'answer', val: ans });
  } else if (d.kind === 'answer') {
    const p = _rtcPeers[d.from];
    if (p?.pc) {
      await p.pc.setRemoteDescription(new RTCSessionDescription(d.val));
      while (p.pendingIce.length) await p.pc.addIceCandidate(new RTCIceCandidate(p.pendingIce.shift()));
    }
  } else if (d.kind === 'candidate') {
    const p = _rtcPeers[d.from] || getOrCreatePeer(d.from, false);
    if (p.pc.remoteDescription) await p.pc.addIceCandidate(new RTCIceCandidate(d.val));
    else p.pendingIce.push(d.val);
  }
}

let bc;
const initBC=()=>{
  try{
    bc=new BroadcastChannel('muji4');
    bc.onmessage=e=>onSig(e.data);
  }catch(e){}
  initSig();
  ann();
  setInterval(ann,3200);
  setInterval(()=>{
    const before=S.nb.length;
    S.nb=S.nb.filter(p=>now()-p.seen<14000);
    if(S.nb.length!==before)updateOnlineUI();
  },4500);
};

const tx = d => {
  try { bc?.postMessage(d); } catch(e){}
  try {
    if (d.to && d.to !== 'public') {
      const direct = rtcSend(d.to, d);
      if (!direct) sigTx(d);
    } else {
      let activeCount = 0;
      Object.keys(_rtcPeers).forEach(id => { if (rtcSend(id, d)) activeCount++; });
      sigTx(d);
    }
  } catch(e){}
};

const ann=()=>{if(!S.me)return;S.cfg.la[S.me.id]=now();tx({t:'a',id:S.me.id,n:S.me.name,av:S.me.av,bio:S.me.bio,lk:S.me.link,sg:S.me.song,ts:now()})};
const blk=id=>S.bl.some(b=>b.id===id);

const NETKEY=LK+'netbox';
const netRead=()=>{try{return JSON.parse(localStorage.getItem(NETKEY)||'[]')}catch(e){return[]}};
const netWrite=arr=>{try{localStorage.setItem(NETKEY,JSON.stringify(arr.slice(-600)))}catch(e){}};
const markSeen=id=>{if(!S.netSeen.includes(id)){S.netSeen.push(id);if(S.netSeen.length>1200)S.netSeen=S.netSeen.slice(-1200);save()}};
const netPush=env=>{const arr=netRead();arr.push(env);netWrite(arr);tx({t:'net',env})};
function netApply(env){
if(!env||!env.id||S.netSeen.includes(env.id))return;
if(!S.me||env.from===S.me.id){markSeen(env.id);return}
if(blk(env.from)){markSeen(env.id);return}
if(env.to!=='public'&&env.to!==S.me.id){return}
if(env.type==='pub'){pms.push(env.payload);if(pms.length>350)pms.shift();if(_view==='public'&&_pubSeg==='chat'&&!_sub)rPub()}
else if(env.type==='dm'){if(!S.co[env.from])S.co[env.from]={m:[],lr:0};S.co[env.from].m.push(env.payload);save();
if(_cid===env.from&&_sub==='c'){rConvo();scr($('#cms'))}else{updBdg()}}
else if(env.type==='note'){S.no.pe[env.from]={txt:env.payload.txt,sg:env.payload.sg,name:env.fromName,av:env.fromAv,ts:env.payload.ts,lk:env.payload.lk||0};if(_view==='notes'&&!_sub)withScroll('#notesScr',()=>rView({enter:false}));rPips?.()}
else if(env.type==='post'){if(!S.feed.find(p=>p.id===env.payload.id)){S.feed.unshift(env.payload);S.feed=S.feed.slice(0,80);if(_view==='public'&&_pubSeg==='feed'&&!_sub)rFeedList()}}
markSeen(env.id);save()}
function netPoll(){netRead().forEach(netApply)}

let pms=[];
let _view='public',_sub=null,_cid=null,_att={},_reply=null,_mt=null,_mid=null,_toastN=0,_pubSeg='chat';

function onSig(d){
if(!S.me)return;
if(d.t==='_rtc_sig'){handleRtcSignal(d);return}
if(d.id===S.me.id||blk(d.id))return;
if(d.t==='net'){netApply(d.env);return}
if(d.t==='a'){
const i=S.nb.findIndex(p=>p.id===d.id);const existing=i>=0?S.nb[i]:null;
const isNew=!existing;const changed=isNew||existing.name!==d.n||existing.av!==d.av||existing.bio!==d.bio||existing.sg?.title!==d.sg?.title;
const p={id:d.id,name:d.n,av:d.av,bio:d.bio,lk:d.lk,sg:d.sg,seen:d.ts};
if(i>=0)S.nb[i]=p;else S.nb.push(p);
if(isNew){if(!S.fr.find(f=>f.id===d.id)){S.fr.push({id:d.id,name:d.n,av:d.av,at:now()});if(!S.co[d.id])S.co[d.id]={m:[],lr:0};save()}}
if(S.me.id>d.id&&!_rtcPeers[d.id]){getOrCreatePeer(d.id,true)}
if(changed&&!_sub){
if(_view==='public')updateOnlineUI();
else if(_view==='people')withScroll('#ppl',()=>rView({enter:false}));
else if(_view==='chats')withScroll('#chl',()=>rView({enter:false}));
}
return}
if(d.t==='p'){pms.push(d);if(pms.length>350)pms.shift();markSeen(d.mid);if(_view==='public'&&_pubSeg==='chat'&&!_sub)rPub();else{chime();banner(d.n,d.txt||'Sent media')}}
else if(d.t==='dm'&&d.to===S.me.id){if(!S.co[d.id])S.co[d.id]={m:[],lr:0};
S.co[d.id].m.push({id:d.mid,fr:d.id,txt:d.txt,mt:d.mt||'text',med:d.med,fn:d.fn,fs:d.fs,ts:d.ts||now(),st:'delivered',rp:d.rp});markSeen(d.mid);save();
if(_cid===d.id&&_sub==='c'){rConvo();scr($('#cms'));txRd(d.id)}else{chime();vib([10,40,10]);banner(S.fr.find(f=>f.id===d.id)?.name||sid(d.id),d.mt==='text'?d.txt:'Sent media');updBdg()}}
else if(d.t==='tp'&&d.to===S.me.id){const el=$('#typ');if(el&&_cid===d.id){el.classList.add('on');clearTimeout(el._t);el._t=setTimeout(()=>el.classList.remove('on'),2600)}}
else if(d.t==='rd'&&d.to===S.me.id){const c=S.co[d.id];if(c)c.m.forEach(m=>{if(m.fr===S.me.id&&m.ts<=d.up){m.st='read';m.rt=d.rt}});save();if(_cid===d.id)rConvo()}
else if(d.t==='ed'&&d.to===S.me.id){const c=S.co[d.id];if(c){const m=c.m.find(m=>m.id===d.mid);if(m){m.txt=d.nt;m.ed=1;save();if(_cid===d.id)rConvo()}}}
else if(d.t==='us'&&d.to===S.me.id){const c=S.co[d.id];if(c){const m=c.m.find(m=>m.id===d.mid);if(m){m.rm=1;m.txt='';m.med=null;save();if(_cid===d.id)rConvo()}}}
else if(d.t==='rx'&&d.to===S.me.id){const c=S.co[d.id];if(c){const m=c.m.find(m=>m.id===d.mid);if(m){m.hrt=d.hrt;save();if(_cid===d.id)rConvo()}}}
else if(d.t==='nt'){S.no.pe[d.id]={txt:d.nt,sg:d.sg,name:d.n,av:d.av,ts:d.ts,lk:d.lk||0};markSeen(d.nid);if(_view==='notes'&&!_sub)withScroll('#notesScr',()=>rView({enter:false}));rPips?.()}
}
function txRd(pid){if(!S.cfg.rcpt)return;const c=S.co[pid];if(!c)return;const l=c.m.filter(m=>m.fr===pid).pop();if(!l)return;tx({t:'rd',id:S.me.id,to:pid,up:l.ts,rt:now()});c.lr=now();save()}

function withScroll(sel,fn){const el=$(sel);const y=el?el.scrollTop:0;fn();requestAnimationFrame(()=>{const el2=$(sel);if(el2)el2.scrollTop=y})}
function updateOnlineUI(){const el=$('#pubOnline');if(el)el.textContent=(S.nb.length+1)+' online'}

const BB=[];

function botHash(v){
let h=2166136261;
v=String(v??'');
for(let i=0;i<v.length;i++){h^=v.charCodeAt(i);h=Math.imul(h,16777619)}
return h>>>0
}

function botRand(v){
return(botHash(v)%1000000)/1000000
}

function botPick(a,v){
return a?.length?a[Math.floor(botRand(v)*a.length)]:null
}

function botClean(v){
return String(v??'').replace(/\s+/g,' ').trim()
}

function botUnique(a){
return[...new Set((a||[]).filter(Boolean))]
}

function botWords(v){
return botClean(v).toLowerCase().replace(/[^a-z0-9\s'-]/g,'').split(/\s+/).filter(x=>x.length>2)
}

function botRemember(b,type,value,max=30){
if(!b.history)b.history={};
if(!Array.isArray(b.history[type]))b.history[type]=[];
if(value!=null)b.history[type].push(value);
b.history[type]=b.history[type].slice(-max)
}

function botSeen(b,type,value){
return!!(b.history?.[type]||[]).includes(value)
}

function botInterests(u){
const seed=[
u.login?.uuid,
u.name?.first,
u.name?.last,
u.gender,
u.nat,
u.location?.city,
u.location?.state,
u.location?.country,
u.location?.timezone?.offset,
u.dob?.age
].filter(Boolean).join('|');

const fields=[
'music','film','games','technology','science','space','photography',
'art','design','books','history','nature','weather','travel','food',
'sports','fashion','cars','architecture','psychology','culture',
'writing','internet','animals','coffee','cooking','fitness'
];

return fields.map((x,i)=>({x,n:botRand(seed+'|interest|'+x+'|'+i)}))
.sort((a,b)=>b.n-a.n)
.slice(0,4+Math.floor(botRand(seed+'|count')*3))
.map(x=>x.x)
}

function botPersonality(u,topics){
const seed=(u.login?.uuid||u.email||u.name?.first||'')+'|'+topics.join('|');

const traits=[
'curious','observant','funny','reflective','analytical','enthusiastic',
'quiet','blunt','playful','nostalgic','skeptical','optimistic',
'detail-oriented','sarcastic','thoughtful','adventurous','random',
'opinionated','calm','intense'
];

const ranked=traits.map(x=>({x,n:botRand(seed+x)})).sort((a,b)=>b.n-a.n);

return{
traits:ranked.slice(0,4).map(x=>x.x),
voice:botPick(['casual','conversational','dry','warm','short','descriptive','opinionated'],seed+'voice'),
humor:botRand(seed+'humor'),
curiosity:botRand(seed+'curiosity'),
storytelling:botRand(seed+'storytelling'),
questioning:botRand(seed+'questioning'),
posting:botPick(['occasional','regular','active'],seed+'posting')
}
}

function botBuildBio(u,topics,p){
const seed=u.login?.uuid||u.email||u.name?.first||'';
const t=topics.slice(0,3);

const openings=[
'Mostly here to find interesting things.',
'Collecting things worth remembering.',
'I tend to disappear into whatever catches my attention.',
'Usually just following whatever rabbit hole looks interesting.',
'Here for good conversations and interesting distractions.',
'Probably thinking about something unnecessarily specific.',
'Just seeing what I run into.'
];

const fragments=[
t.length?'Lately that has been '+t.join(', ')+'.':'',
p.traits.includes('curious')?'I ask too many questions.':'',
p.traits.includes('observant')?'I notice weird little details.':'',
p.traits.includes('funny')?'Occasionally funny on purpose.':'',
p.traits.includes('reflective')?'I think about things longer than I probably should.':'',
p.traits.includes('analytical')?'I like figuring out how things work.':'',
p.traits.includes('adventurous')?'Always interested in trying something different.':'',
p.traits.includes('nostalgic')?'Probably has an old favorite for everything.':''
];

return botClean(botPick(openings,seed+'open')+' '+botPick(botUnique(fragments),seed+'frag'))
}

function botHandle(u){
const x=botClean(u.login?.username);
return(x||('user'+botHash(u.login?.uuid||Math.random()))).replace(/[^a-zA-Z0-9_.-]/g,'').slice(0,28)
}

function botBuildData(u,i){
const sourceId=u.login?.uuid||u.email||String(i);
const topics=botInterests(u);
const personality=botPersonality(u,topics);

return{
id:uid(),
sourceId,
name:botClean(u.name?.first)||'User',
fullName:botClean([u.name?.first,u.name?.last].filter(Boolean).join(' ')),
title:botClean(u.name?.title),
username:botHandle(u),
bio:botBuildBio(u,topics,personality),
av:toHttps(u.picture?.large||u.picture?.medium||u.picture?.thumbnail||''),
thumb:toHttps(u.picture?.thumbnail||''),
gender:u.gender||null,
nat:u.nat||null,
age:u.dob?.age||null,
dob:u.dob?.date||null,
registered:u.registered?.date||null,
email:u.email||null,
phone:u.cell||u.phone||null,
location:u.location||{},
identity:u.id||{},
login:{uuid:u.login?.uuid||'',username:u.login?.username||''},
topics,
interest:topics[0]||'general',
personality,
tone:personality.voice,
activity:{
frequency:1+Math.floor(botRand(sourceId+'frequency')*5),
hour:Math.floor(botRand(sourceId+'hour')*24),
lastActive:now()-Math.floor(botRand(sourceId+'active')*172800000),
posts:Math.floor(botRand(sourceId+'posts')*40),
likes:5+Math.floor(botRand(sourceId+'likes')*100)
},
history:{
topics:[],
content:[],
captions:[],
music:[],
formats:[],
questions:[]
},
source:'randomuser',
ct:now(),
meta:{
name:u.name||{},
location:u.location||{},
login:{uuid:u.login?.uuid||'',username:u.login?.username||''},
dob:u.dob||{},
registered:u.registered||{},
id:u.id||{},
picture:u.picture||{},
nat:u.nat||null,
gender:u.gender||null
}
}
}

async function fetchRandomUsers(n){
try{
const r=await fetch('https://randomuser.me/api/?results='+n+'&nat=us,gb,ca,au,de,fr&inc=gender,name,location,email,login,dob,registered,phone,cell,id,picture,nat&noinfo');
if(!r.ok)return[];
const d=await r.json();
return Array.isArray(d.results)?d.results:[]
}catch(e){return[]}
}

async function initBots(){
if(!S.bots||S.bots.length<6){
const users=[];
const ids=new Set();
const pfps=new Set();
let tries=0;

while(users.length<8&&tries++<7){
const batch=await fetchRandomUsers(Math.max(8,(8-users.length)*3));

for(const u of batch){
const id=u.login?.uuid||u.email;
const pfp=toHttps(u.picture?.large||u.picture?.medium||u.picture?.thumbnail||'');

if(!id||!pfp||ids.has(id)||pfps.has(pfp))continue;

ids.add(id);
pfps.add(pfp);
users.push(u);

if(users.length>=8)break
}
}

S.bots=users.map(botBuildData);
save()
}

await genBotNotes()
}

function botContentFormat(b){
const formats=[
'story','observation','question','recommendation','opinion',
'discovery','mini-list','reaction','confession','joke',
'conversation','thought','memory','explanation','hot-take'
];

const scored=formats.map(x=>{
let n=botRand(b.sourceId+'|format|'+x);

if(b.personality?.storytelling>.65&&x==='story')n+=.4;
if(b.personality?.questioning>.65&&x==='question')n+=.4;
if(b.personality?.humor>.65&&x==='joke')n+=.45;
if(b.personality?.curiosity>.65&&x==='discovery')n+=.35;

if(botSeen(b,'formats',x))n-=.3;

return{x,n}
}).sort((a,b)=>b.n-a.n);

return scored[0]?.x||'thought'
}

function botTopic(b,extra=''){
const pool=botUnique([
...(b.topics||[]),
...botWords(extra),
'things I noticed',
'something interesting',
'whatever I found'
]);

return botPick(pool,b.sourceId+'|topic|'+Date.now())
}

function botNaturalQuestion(b,subject){
const q=[
'Has anyone else noticed this?',
'Does anyone else think this is underrated?',
'What is everyone listening to lately?',
'What is the first thing you would do here?',
'Would you actually try this?',
'Why does nobody talk about this?',
'What is your favorite version of this?',
'Is this actually good or am I missing something?',
'What is something similar I should check out?',
'Anyone have a better recommendation?',
'Would you keep this or change it?',
'What is one thing you could talk about forever?'
];

return botPick(q,b.sourceId+'|question|'+subject)
}

function botOpinion(b,subject){
const openings=[
'I did not expect to like this as much as I do.',
'I think this is way more interesting than people give it credit for.',
'I might be completely wrong, but',
'I keep coming back to this because',
'Honestly,',
'The weird thing about this is',
'I cannot decide whether',
'Maybe it is just me, but'
];

return botPick(openings,b.sourceId+'|opinion|'+subject)
}

function botGenerateText(b,format,subject,data){
const title=botClean(data?.title);
const description=botClean(data?.description||data?.explanation);
const text=botClean(data?.text);
const value=title||description||text||subject||'this';

if(format==='question')return botNaturalQuestion(b,value);

if(format==='opinion')return botClean(botOpinion(b,value)+' '+botPick([
'value is genuinely interesting.',
'value feels strangely underrated.',
'value has been on my mind.',
'value is worth looking into.',
'value is one of those things I cannot stop thinking about.'
],b.sourceId+value));

if(format==='recommendation')return botPick([
'If you like '+subject+', this is probably worth checking out.',
'Adding this to the list of things I want to spend more time with.',
'This is going into my recommendations pile.',
'Found this while looking for '+subject+'. Actually worth the detour.'
],b.sourceId+value);

if(format==='observation')return botPick([
'The interesting part is not even the main thing here.',
'There is a weird amount of detail packed into this.',
'I kept looking at this longer than I expected.',
'Something about this feels oddly familiar.',
'The small details are what got me.',
'This is the kind of thing that makes you stop scrolling.'
],b.sourceId+value);

if(format==='discovery')return botPick([
'Okay, I had no idea this existed.',
'Just found this and immediately went down a rabbit hole.',
'This sent me through about five related searches.',
'Accidentally learned something new today.',
'This was not what I expected to find.'
],b.sourceId+value);

if(format==='mini-list'){
const lists=[
'Three things this reminded me of: '+subject+', something completely different, and now another rabbit hole.',
'Things I want to check next: '+subject+', related stuff, and whatever everyone recommends.',
'Current rabbit holes: '+subject+', '+botTopic(b,subject)+', and probably one more thing by tonight.'
];
return botPick(lists,b.sourceId+value)
}

if(format==='reaction')return botPick([
'Okay, this one got me.',
'This is actually pretty good.',
'I was not prepared for that.',
'That is genuinely cool.',
'Yeah, I can see why people are talking about this.',
'Not sure what I expected, but definitely not that.'
],b.sourceId+value);

if(format==='confession')return botPick([
'I have spent way too much time looking into this.',
'I probably did not need to know this much about it.',
'I opened this for a minute and somehow lost an hour.',
'I said I would only look for a second. That did not happen.'
],b.sourceId+value);

if(format==='conversation')return botNaturalQuestion(b,value);

if(format==='story'){
const stories=[
'Went looking for something completely unrelated and somehow ended up here. '+value+'.',
'This started as a tiny curiosity and turned into a full rabbit hole. '+value+'.',
'I only meant to check this for a second. '+value+'. Now I have about ten tabs open.',
'There is always one random thing that completely derails the day. Today it was '+value+'.'
];
return botPick(stories,b.sourceId+value)
}

if(format==='joke')return botPick([
'I came here for '+subject+' and left with twelve tabs open.',
'Me: I will look at this for thirty seconds. Also me an hour later: still here.',
'Apparently my hobby is accidentally becoming an expert in random things.'
],b.sourceId+value);

if(format==='explanation')return description||title||'This one is worth looking into.';

if(format==='memory')return botPick([
'This weirdly reminded me of something from years ago.',
'This brought back a very specific memory.',
'I have not thought about this in forever.',
'This feels oddly nostalgic.'
],b.sourceId+value);

return botPick([
'This has been living in my head for a minute.',
'Interesting enough that I wanted to save it here.',
'This deserved more than a quick scroll.',
'Adding this to the mental collection.',
'Found something worth sharing.'
],b.sourceId+value)
}

function botMusicQueries(b){
const topics=b.topics||[];
const traits=b.personality?.traits||[];
const queries=[];

const maps={
music:[
'underground music',
'emerging artists',
'new independent music',
'deep cuts',
'underrated albums',
'album recommendations',
'new releases',
'live sessions',
'artist discoveries'
],
film:[
'film soundtrack',
'cinematic music',
'movie score',
'film recommendations',
'cult movie soundtrack',
'independent film music'
],
games:[
'video game soundtrack',
'game music',
'indie game soundtrack',
'retro game music',
'game score'
],
art:[
'experimental electronic',
'art pop',
'ambient experimental',
'visual art soundtrack'
],
travel:[
'world music',
'global indie',
'travel music',
'regional music discovery'
],
nature:[
'atmospheric music',
'field recording',
'organic ambient',
'nature inspired music'
],
technology:[
'electronic music',
'IDM',
'glitch electronic',
'experimental electronic',
'future garage'
],
space:[
'cosmic ambient',
'space music',
'astral electronic',
'cinematic ambient'
],
books:[
'literary music',
'acoustic indie',
'neo classical',
'ambient reading music'
],
psychology:[
'trip hop',
'minimal electronic',
'dark jazz',
'experimental R&B'
],
culture:[
'global music discovery',
'regional artists',
'folk fusion',
'modern traditional music'
]
};

for(const t of topics)if(maps[t])queries.push(...maps[t]);

if(traits.includes('curious'))queries.push(
'genre blending',
'unknown artists',
'obscure albums',
'one hit wonders',
'forgotten artists'
);

if(traits.includes('nostalgic'))queries.push(
'2000s alternative',
'90s indie',
'retro soundtrack',
'old album rediscovery'
);

if(traits.includes('analytical'))queries.push(
'progressive rock',
'jazz fusion',
'complex electronic',
'concept album'
);

if(traits.includes('playful'))queries.push(
'electro swing',
'dance punk',
'hyperpop',
'funk fusion'
);

return botUnique(queries)
}

async function botFindMusic(b){
const queries=botMusicQueries(b);

for(let i=0;i<Math.min(5,queries.length);i++){
const q=botPick(queries,b.sourceId+'|musicquery|'+i+'|'+Math.floor(now()/3600000));

try{
const ss=await searchSongs(q);
const usable=(ss||[]).filter(s=>{
const key=(s.title||'')+'|'+(s.artist?.name||'');
return s.title&&!botSeen(b,'music',key)
});

if(!usable.length)continue;

const s=botPick(usable,b.sourceId+'|track|'+q+'|'+Date.now());
const key=(s.title||'')+'|'+(s.artist?.name||'');

botRemember(b,'music',key,40);

return{
title:s.title||'',
artist:s.artist?.name||'',
art:toHttps(s.album?.cover_medium||s.album?.cover||''),
preview:toHttps(s.preview||''),
query:q
}
}catch(e){}
}

return null
}

async function genBotNotes(){
if(!S.bots?.length)return;

for(const b of S.bots){
if(S.no.pe[b.id]&&now()-S.no.pe[b.id].ts<3600000)continue;

const format=botContentFormat(b);
const topic=botTopic(b);
let txt='';
let sg=null;

try{
const mode=botRand(b.sourceId+'|note|'+Math.floor(now()/3600000));

if(mode<.22){
const r=await fetch('https://uselessfacts.jsph.pl/api/v2/facts/random?language=en');

if(r.ok){
const d=await r.json();
const fact=botClean(d.text);

txt=botGenerateText(b,
botPick(['discovery','observation','reaction','explanation'],b.sourceId+'fact'),
topic,{text:fact,title:fact}
)
}
}else if(mode<.38){
const r=await fetch('https://v2.jokeapi.dev/joke/Any?type=single&safe-mode');

if(r.ok){
const d=await r.json();
if(d?.joke)txt=d.joke
}
}else{
txt=botGenerateText(b,format,topic,{title:topic})
}

if(!txt)txt=botGenerateText(b,'thought',topic,{title:topic});

if(botRand(b.sourceId+'|songnote|'+Date.now())>.35){
sg=await botFindMusic(b)
}
}catch(e){}

txt=botClean(txt);

S.no.pe[b.id]={
txt,
sg,
name:b.name,
av:b.av,
username:b.username,
ts:now()-Math.floor(botRand(b.sourceId+'|notetime|'+Date.now())*3600000),
lk:botRand(b.sourceId+'|notelikes')>.68?1+Math.floor(botRand(b.sourceId+'|likecount')*8):0
};

botRemember(b,'formats',format);
botRemember(b,'topics',topic);
botRemember(b,'content',txt);
}

save();
rPips?.();

if(_view==='notes'&&!_sub)withScroll('#notesScr',()=>rView({enter:false}))
}

const FEEDCATS=['space','tech','weather','news'];
let _fcIdx=0;

async function genBotFeedBatch(){
if(!S.bots?.length)return;

const bot=S.bots[Math.floor(Math.random()*S.bots.length)];
const cat=botPick(FEEDCATS,bot.sourceId+'|feedcat|'+Date.now());

let img=null;
let data=null;
let raw='';
let contentKey='';

try{
if(cat==='space'){
const r=await fetch('https://api.nasa.gov/planetary/apod?api_key=DEMO_KEY');

if(r.ok){
const d=await r.json();

if(d?.media_type==='image'){
data=d;
img=toHttps(d.hdurl||d.url||'');
raw=d.title||d.explanation||'';
contentKey='space|'+(d.date||'')+'|'+(d.title||'')
}
}
}

else if(cat==='tech'||cat==='news'){
const r=await fetch('https://hacker-news.firebaseio.com/v0/topstories.json');

if(r.ok){
const ids=await r.json();
const pool=(ids||[]).slice(0,80);

for(let i=0;i<12&&pool.length;i++){
const pick=pool[Math.floor(botRand(bot.sourceId+'|hn|'+i+'|'+Date.now())*pool.length)];
const sr=await fetch('https://hacker-news.firebaseio.com/v0/item/'+pick+'.json');

if(!sr.ok)continue;

const s=await sr.json();
if(!s?.title)continue;

const key=cat+'|'+pick;
if(botSeen(bot,'content',key))continue;

data=s;
raw=s.title;
contentKey=key;
break
}
}
}

else if(cat==='weather'){
const loc=bot.location;

if(loc?.coordinates?.latitude&&loc?.coordinates?.longitude){
const r=await fetch(
'https://api.open-meteo.com/v1/forecast?latitude='+
loc.coordinates.latitude+
'&longitude='+
loc.coordinates.longitude+
'&current_weather=true'
);

if(r.ok){
const d=await r.json();

data={
city:loc.city||loc.country||'there',
temperature:d?.current_weather?.temperature,
weather:d?.current_weather?.weathercode
};

raw=data.city+' '+data.temperature;
contentKey='weather|'+loc.city+'|'+Math.floor(now()/10800000)
}
}
}
}catch(e){}

if(!raw)return;

let format=botContentFormat(bot);
let subject=cat==='weather'?'weather':botTopic(bot,raw);

if(cat==='space'){
format=botPick([
'discovery','observation','story','reaction','explanation','question'
],bot.sourceId+'spaceformat'+contentKey)
}

if(cat==='tech'){
format=botPick([
'opinion','discovery','explanation','reaction','question','hot-take'
],bot.sourceId+'techformat'+contentKey)
}

if(cat==='news'){
format=botPick([
'reaction','opinion','question','observation','conversation'
],bot.sourceId+'newsformat'+contentKey)
}

if(cat==='weather'){
format=botPick([
'observation','question','conversation','reaction'
],bot.sourceId+'weatherformat'+contentKey)
}

let caption=botGenerateText(bot,format,subject,data);

if(cat==='space'&&data?.explanation){
const extra=botClean(data.explanation).slice(0,420);

if(format==='explanation'){
caption=extra;
}else if(format==='discovery'){
caption=botClean(caption+' '+extra.slice(0,180));
}
}

if(cat==='news'&&data?.title){
caption=botClean(
botGenerateText(bot,format,data.title,data)
)
}

if(cat==='weather'&&data?.temperature!=null){
const place=data.city;
const temp=Math.round(data.temperature);

caption=botPick([
place+' is sitting around '+temp+'°C right now.',
'Weather check: '+place+', '+temp+'°C.',
'Apparently it is '+temp+'°C in '+place+'.',
'Checking the weather in '+place+'. '+temp+'°C today.'
],bot.sourceId+'weathercaption'+contentKey)
}

caption=botClean(caption);

if(!caption||botSeen(bot,'captions',caption))return;

const post={
id:uid(),
type:'bot',
authorId:bot.id,
authorName:bot.name,
authorAv:bot.av,
authorUsername:bot.username,
img,
caption,
cat,
ts:now()-Math.floor(botRand(bot.sourceId+'|posttime|'+contentKey)*5400000),
likes:Math.floor(botRand(bot.sourceId+'|likes|'+contentKey)*Math.max(8,bot.activity?.likes||30)),
likedByMe:false,
meta:{
format,
topic:subject,
source:contentKey,
botTraits:bot.personality?.traits||[]
}
};

botRemember(bot,'content',contentKey);
botRemember(bot,'captions',caption);
botRemember(bot,'formats',format);
botRemember(bot,'topics',subject);

S.feed.unshift(post);
S.feed=S.feed.slice(0,80);
save();

if(_view==='public'&&_pubSeg==='feed'&&!_sub){
withScroll('#feedScr',()=>rFeedList())
}
}

async function seedFeedIfEmpty(){
if(S.feed.length)return;

const target=Math.min(8,S.bots?.length||0);

for(let i=0;i<target;i++){
await genBotFeedBatch()
}
}
function seedPub(){if(pms.length)return;
pms.push({id:'sys',n:'MUJI',txt:'Welcome to public chat. Everyone nearby can see messages here.',mt:'text',mid:uid(),ts:now()-50000});
}

async function searchSongs(q){return new Promise(res=>{const cb='_dz'+now()+(Math.random()*1e4|0);window[cb]=d=>{delete window[cb];try{document.body.removeChild(sc)}catch(e){};res(d?.data||[])};
const sc=document.createElement('script');sc.src='https://api.deezer.com/search?q='+encodeURIComponent(q)+'&limit=12&output=jsonp&callback='+cb;sc.onerror=()=>{delete window[cb];res([])};document.body.appendChild(sc)})}

function rView(opts){
const el=$('#views');if(!el)return;
if(_sub==='c'){rConvoVw(opts);return}
if(_sub==='pr'){rProfVw(opts);return}
if(_sub==='nv'){rNoteVw(opts);return}
if(_view==='public')rPubVw(el,opts);
else if(_view==='chats')rChatsVw(el,opts);
else if(_view==='notes')rNotesVw(el,opts);
else if(_view==='people')rPplVw(el,opts);
else if(_view==='me')rMeVw(el,opts);
}
function vwClass(opts){return opts?.enter==='stack'?'vw on stack-enter':(opts?.enter===false?'vw on':'vw on tab-fade')}
function emptyState(kind,title,sub){
const icons={chat:'<path d="M21 15a2 2 0 01-2 2H7l-4 4V5a2 2 0 012-2h14a2 2 0 012 2z"/>',
chats:'<path d="M21 11.5a8.38 8.38 0 01-.9 3.8 8.5 8.5 0 01-7.6 4.7 8.38 8.38 0 01-3.8-.9L3 21l1.9-5.7a8.38 8.38 0 01-.9-3.8 8.5 8.5 0 014.7-7.6 8.38 8.38 0 013.8-.9h.5a8.48 8.48 0 018 8v.5z"/>',
notes:'<path d="M9 18V5l12-2v13M6 18a3 3 0 100-6 3 3 0 000 6zM18 16a3 3 0 100-6 3 3 0 000 6z"/>'};
return '<div class="emptyw"><svg viewBox="0 0 24 24">'+(icons[kind]||icons.chat)+'</svg><b>'+san(title)+'</b><span>'+san(sub)+'</span></div>'}
function skRows(n){let h='';for(let i=0;i<n;i++)h+='<div class="skrow"><div class="skav shimmer"></div><div class="sklines"><div class="skline shimmer" style="width:'+(50+Math.random()*30|0)+'%"></div><div class="skline shimmer" style="width:'+(30+Math.random()*30|0)+'%"></div></div></div>';return h}
function skCards(n){let h='';for(let i=0;i<n;i++)h+='<div class="skcard"><div class="skrow" style="padding-bottom:0"><div class="skav shimmer" style="width:34px;height:34px"></div><div class="sklines"><div class="skline shimmer" style="width:40%"></div></div></div><div class="skimg shimmer"></div><div class="skfoot"><div class="skline shimmer" style="width:60px;height:22px;border-radius:11px"></div></div></div>';return h}
function catLabel(cat){return cat==='space'?'🛰 Space':cat==='tech'?'🖥 Tech':cat==='weather'?'☁ Weather':cat==='news'?'📰 News':'👤 Post'}

function rPubVw(el,opts){
let h='<div class="'+vwClass(opts)+'" id="vpub"><div class="tb"><div class="tbc"><div class="tbt">Public</div><div class="tbs" id="pubOnline">'+(S.nb.length+1)+' online</div></div></div>';
h+='<div class="seg"><button class="segb '+(_pubSeg==='chat'?'on':'')+'" data-act="pubSeg" data-seg="chat">Chat</button><button class="segb '+(_pubSeg==='feed'?'on':'')+'" data-act="pubSeg" data-seg="feed">Feed</button></div>';
if(_pubSeg==='chat'){
h+='<div class="ma" id="pms"></div><div class="typ" id="ptyp"><i></i><i></i><i></i><span>typing</span></div>';
h+='<div class="atp" id="patp"></div>';
h+='<div class="ib"><button data-act="attach" data-ctx="pub"><svg viewBox="0 0 24 24"><path d="M12 5v14M5 12h14"/></svg></button>';
h+='<textarea id="pin" placeholder="Message..." rows="1" data-oninp="pub"></textarea>';
h+='<button class="snd" id="psnd" data-act="sendPub"><svg viewBox="0 0 24 24"><line x1="22" y1="2" x2="11" y2="13"/><polygon points="22 2 15 22 11 13 2 9 22 2"/></svg></button></div></div>';
}else{
h+='<div class="feedhead"><span class="rfr" id="feedRefreshBtn" data-act="refreshFeed"><svg viewBox="0 0 24 24"><path d="M23 4v6h-6M1 20v-6h6"/><path d="M3.51 9a9 9 0 0114.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0020.49 15"/></svg>Refresh</span><button class="npostbtn" data-act="newPost"><svg viewBox="0 0 24 24"><path d="M12 5v14M5 12h14"/></svg></button></div>';
h+='<div class="scr" id="feedScr"><div id="feedList"></div></div>';
}
el.innerHTML=h;
if(_pubSeg==='chat'){rPub();setTimeout(()=>scr($('#pms')),50);bindTextarea($('#pin'),'pub')}
else{rFeedList()}
}

function rPub(){const el=$('#pms');if(!el)return;let h='',ld='';
if(!pms.length){el.innerHTML=emptyState('chat','No messages yet','Say hello — everyone nearby can see this');return}
pms.forEach(m=>{if(m.hid)return;const d=df(m.ts);if(d!==ld){h+='<div class="md"><span class="mdp">'+d+'</span></div>';ld=d}
if(m.id==='sys'){h+='<div class="sysmsg"><span>'+san(m.txt)+'</span></div>';return}
const me=m.id===S.me?.id;h+='<div class="mr '+(me?'s':'r')+' ani">';
if(!me)h+='<div class="mg">'+san(m.n||sid(m.id))+'</div>';
if(m.rm)h+='<div class="mrm"><svg viewBox="0 0 24 24"><path d="M3 6h18M19 6v14a2 2 0 01-2 2H7a2 2 0 01-2-2V6"/></svg>Message removed</div>';
else{h+='<div class="mb">';if(m.rp)h+='<div class="mreply">'+san(m.rp)+'</div>';
if(m.mt==='image'&&m.med)h+='<div class="mimg" data-act="view" data-src="'+encodeURIComponent(m.med)+'"><img src="'+m.med+'"></div>';
else h+='<div class="mt">'+lnk(san(m.txt||''))+'</div>';
if(m.fn&&m.med&&m.mt==='file')h+='<div class="matt"><svg viewBox="0 0 24 24"><path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z"/><path d="M14 2v6h6"/></svg><div><div class="mfn">'+san(m.fn)+'</div><div class="mfs">'+fz(m.fs||0)+'</div></div></div>';
h+='<div class="mm"><span class="mts">'+tf(m.ts)+'</span></div></div>'}h+='</div>'});
el.innerHTML=h}

function rFeedList(){const el=$('#feedList');if(!el)return;
if(!S.feed.length){el.innerHTML=skCards(3);seedFeedIfEmpty();return}
let h='';S.feed.forEach(p=>{
h+='<div class="postcard" data-postid="'+p.id+'"><div class="posthead"><div class="av sm">'+AV(p.authorAv)+'</div><div class="lb"><div class="ln">'+san(p.authorName)+'</div><div class="ls">'+rt(p.ts)+' ago</div></div><span class="catchip">'+catLabel(p.cat)+'</span></div>';
h+='<div class="postimg"><img src="'+p.img+'" loading="lazy" onerror="this.closest(\'.postimg\').style.background=\'var(--s3)\';this.remove()"></div>';
h+='<div class="postact"><button class="heartbtn '+(p.likedByMe?'on':'')+'" data-act="likePost" data-id="'+p.id+'"><svg viewBox="0 0 24 24"><path d="M20.8 4.6a5.5 5.5 0 00-7.8 0L12 5.6l-1-1a5.5 5.5 0 10-7.8 7.8l1 1L12 21l7.8-7.6 1-1a5.5 5.5 0 000-7.8z"/></svg></button><span class="likecount" id="lc-'+p.id+'">'+p.likes+'</span></div>';
h+='<div class="postcap"><b>'+san(p.authorName)+'</b>'+san(p.caption)+'</div></div>'});
el.innerHTML=h}

function rChatsVw(el,opts){let h='<div class="'+vwClass(opts)+'"><div class="tb"><div class="tbc"><div class="tbt">Chats</div></div></div>';
h+='<div class="srch"><div class="srchbox"><svg class="sicon" viewBox="0 0 24 24"><circle cx="11" cy="11" r="8"/><path d="M21 21l-4.35-4.35"/></svg><input placeholder="Search" data-act="filterChats"></div></div>';
h+='<div class="scr" id="chl">';
if(S.rq.length){h+='<div class="sh">Requests ('+S.rq.length+')</div>';
S.rq.forEach(r=>{h+='<div class="li"><div class="av">'+AV(r.av)+'</div><div class="lb"><div class="ln">'+san(r.name)+'</div><div class="ls">'+sid(r.id)+'</div></div><div style="display:flex;gap:5px"><button class="tbb" style="background:var(--s3)" data-act="acceptReq" data-id="'+r.id+'"><svg viewBox="0 0 24 24"><path d="M20 6L9 17l-5-5"/></svg></button><button class="tbb" data-act="declineReq" data-id="'+r.id+'"><svg viewBox="0 0 24 24"><path d="M18 6L6 18M6 6l12 12"/></svg></button></div></div>'})}
const en=Object.entries(S.co).map(([p,c])=>({p,f:S.fr.find(f=>f.id===p),l:c.m.filter(m=>!m.hid).pop(),c})).filter(e=>e.l).sort((a,b)=>(b.l?.ts||0)-(a.l?.ts||0));
if(en.length){en.forEach(e=>{const nm=e.f?.name||sid(e.p);const on=S.nb.find(p=>p.id===e.p);
const ur=e.c.m.filter(m=>m.fr!==S.me?.id&&m.ts>(e.c.lr||0)&&!m.hid).length;
let pv='';if(e.l.rm)pv='Message removed';else if(e.l.mt==='image')pv='📷 Photo';else if(e.l.mt==='video')pv='🎬 Video';else if(e.l.mt==='file')pv='📄 '+(e.l.fn||'File');else pv=e.l.txt||'';
if(e.l.fr===S.me?.id)pv='You: '+pv;
h+='<div class="li" data-act="openChat" data-id="'+e.p+'"><div class="av'+(on?' online':'')+'">'+AV(e.f?.av)+'</div><div class="lb"><div class="ln">'+san(nm)+'</div><div class="ls'+(ur?' unread':'')+'">'+san(pv.slice(0,42))+'</div></div><div class="lr"><span class="lt">'+tf(e.l.ts)+'</span>'+(ur?'<span class="bdg">'+ur+'</span>':'')+'</div></div>'})}
else h+=emptyState('chats','No conversations yet','Head to People to find someone nearby to message');
h+='</div></div>';el.innerHTML=h}

function rNotesVw(el,opts){let h='<div class="'+vwClass(opts)+'"><div class="tb"><div class="tbc"><div class="tbt">Notes</div></div></div>';
h+='<div class="npips" id="pips"></div><div class="scr" id="notesScr">';
const all=Object.entries(S.no.pe).filter(([,n])=>n.txt).sort((a,b)=>b[1].ts-a[1].ts);
if(all.length)all.forEach(([id,n])=>{h+='<div class="li" data-act="viewNote" data-id="'+id+'"><div class="av">'+AV(n.av)+'</div><div class="lb"><div class="ln">'+san(n.name||sid(id))+'</div><div class="ls">'+san(n.txt.slice(0,38))+'</div></div><div class="lr"><span class="lt">'+rt(n.ts)+'</span>'+(n.lk?'<span class="ls">❤ '+n.lk+'</span>':'')+'</div></div>'});
else if(!S.bots?.length)h+=skRows(4);
else h+=emptyState('notes','No notes yet','Notes disappear after a while - share what you\'re up to');
h+='</div></div>';el.innerHTML=h;rPips()}

function rPplVw(el,opts){let h='<div class="'+vwClass(opts)+'"><div class="tb"><div class="tbc"><div class="tbt">People</div></div><div class="tbb" data-act="addId"><svg viewBox="0 0 24 24"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg></div></div>';
h+='<div class="srch"><div class="srchbox"><svg class="sicon" viewBox="0 0 24 24"><circle cx="11" cy="11" r="8"/><path d="M21 21l-4.35-4.35"/></svg><input placeholder="Search name or ID" data-act="filterPeople"></div></div>';
h+='<div class="scr" id="ppl">';
const on=S.nb.filter(p=>!blk(p.id));
if(on.length){h+='<div class="sh">Online ('+on.length+')</div>';on.forEach(p=>{h+='<div class="li" data-act="viewProfile" data-id="'+p.id+'"><div class="av online">'+AV(p.av)+'</div><div class="lb"><div class="ln">'+san(p.name)+'</div><div class="ls">'+sid(p.id)+'</div></div><button class="tbb" data-act="openChat" data-id="'+p.id+'" data-stop="1" style="background:var(--s3)"><svg viewBox="0 0 24 24" style="width:14px;height:14px"><path d="M21 15a2 2 0 01-2 2H7l-4 4V5a2 2 0 012-2h14a2 2 0 012 2z"/></svg></button></div>'})}
h+='<div class="sh">Friends ('+S.fr.length+')</div>';
if(S.fr.filter(f=>!blk(f.id)).length===0)h+='<div style="padding:14px;color:var(--t3);font-size:12px">Nearby people you meet will show up here</div>';
S.fr.filter(f=>!blk(f.id)).forEach(f=>{const on2=S.nb.find(p=>p.id===f.id);const la=S.cfg.la[f.id];
h+='<div class="li" data-act="viewProfile" data-id="'+f.id+'"><div class="av'+(on2?' online':'')+'">'+AV(f.av)+'</div><div class="lb"><div class="ln">'+san(f.name)+'</div><div class="ls">'+(on2?'Online':la?rt(la)+' ago':'Offline')+'</div></div></div>'});
if(S.bots?.length){h+='<div class="sh">Community</div>';S.bots.forEach(b=>{h+='<div class="li" data-act="viewProfile" data-id="'+b.id+'"><div class="av">'+AV(b.av)+'</div><div class="lb"><div class="ln">'+san(b.name)+'</div><div class="ls">'+san(b.bio)+'</div></div></div>'})}
else h+=skRows(4);
h+='</div></div>';el.innerHTML=h}

function rMeVw(el,opts){if(!S.me)return;
const totalChats=Object.keys(S.co).length;
let h='<div class="'+vwClass(opts)+'"><div class="tb"><div class="tbc"><div class="tbt">Profile</div></div></div><div class="scr"><div class="pfw">';
h+='<div class="pfavwrap" data-act="editProfile"><div class="pfav">'+AV(S.me.av)+'</div><div class="cam"><svg viewBox="0 0 24 24"><path d="M23 19a2 2 0 01-2 2H3a2 2 0 01-2-2V8a2 2 0 012-2h4l2-3h6l2 3h4a2 2 0 012 2z"/><circle cx="12" cy="13" r="4"/></svg></div></div>';
h+='<div class="pfnm">'+san(S.me.name)+'</div><div class="pfid" data-act="copyId">'+sid(S.me.id)+'</div>';
if(S.me.bio)h+='<div class="pfbio">'+san(S.me.bio)+'</div>';
if(S.me.link)h+='<a class="pflk" href="'+san(S.me.link)+'" target="_blank">'+san(S.me.link.replace(/^https?:\/\//,'').slice(0,28))+'</a>';
if(S.me.song)h+='<div class="pfsong" data-act="playMySong"><div class="pfsa">'+(S.me.song.art?'<img src="'+S.me.song.art+'">':'')+'</div><div style="flex:1;min-width:0"><div class="pfst">'+san(S.me.song.title)+'</div><div class="pfsar">'+san(S.me.song.artist)+'</div></div></div>';
h+='<div class="pfstat"><div><b>'+S.fr.length+'</b><span>Friends</span></div><div><b>'+totalChats+'</b><span>Chats</span></div></div>';
h+='</div><div style="border-top:var(--gb)">';
[['Edit Profile','<path d="M11 4H4a2 2 0 00-2 2v14a2 2 0 002 2h14a2 2 0 002-2v-7"/><path d="M18.5 2.5a2.121 2.121 0 013 3L12 15l-4 1 1-4 9.5-9.5z"/>','editProfile'],
['Profile Song','<path d="M9 18V5l12-2v13M6 18a3 3 0 100-6 3 3 0 000 6z"/>','setSong'],
['Add Link','<path d="M10 13a5 5 0 007.54.54l3-3a5 5 0 00-7.07-7.07l-1.72 1.71"/>','setLink'],
['Privacy','<rect x="3" y="11" width="18" height="11" rx="2"/><path d="M7 11V7a5 5 0 0110 0v4"/>','privacy'],
['Blocked','<circle cx="12" cy="12" r="10"/><path d="M4.93 4.93l14.14 14.14"/>','blockedList'],
['Export Data','<path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/>','exportData'],
['Import Data','<path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4"/><polyline points="17 8 12 3 7 8"/><line x1="12" y1="3" x2="12" y2="15"/>','importData']
].forEach(([l,i,a])=>{h+='<div class="si" data-act="'+a+'"><svg viewBox="0 0 24 24">'+i+'</svg><span>'+l+'</span><svg class="chv" viewBox="0 0 24 24"><path d="M9 18l6-6-6-6"/></svg></div>'});
h+='</div><div style="border-top:var(--gb);margin-top:2px">';
h+='<div class="si dng" data-act="logout"><svg viewBox="0 0 24 24"><path d="M9 21H5a2 2 0 01-2-2V5a2 2 0 012-2h4"/><polyline points="16 17 21 12 16 7"/><line x1="21" y1="12" x2="9" y2="12"/></svg><span>Log Out</span></div>';
h+='<div class="si dng" data-act="clearData"><svg viewBox="0 0 24 24"><path d="M3 6h18M19 6v14a2 2 0 01-2 2H7a2 2 0 01-2-2V6"/></svg><span>Clear All Data</span></div>';
h+='</div></div></div>';el.innerHTML=h}

function rConvoVw(opts){const pid=_cid;if(!pid)return;const f=S.fr.find(f=>f.id===pid);const p=S.nb.find(p=>p.id===pid);const bot=S.bots?.find(b=>b.id===pid);
const nm=f?.name||p?.name||bot?.name||sid(pid);const on=!!p;const la=S.cfg.la[pid];
const el=$('#views');
let h='<div class="'+(opts?.enter===false?'vw on':'vw on stack-enter')+'"><div class="tb"><button class="tbb" data-act="closeConvo"><svg viewBox="0 0 24 24"><path d="M15 18l-6-6 6-6"/></svg></button><div class="tbc" data-act="viewProfile" data-id="'+pid+'"><div class="tbt">'+san(nm)+'</div><div class="tbs">'+(on?'Online':la?rt(la)+' ago':'Offline')+'</div></div><div class="tbb" data-act="openConvoMenu"><svg viewBox="0 0 24 24"><circle cx="12" cy="12" r="1"/><circle cx="12" cy="5" r="1"/><circle cx="12" cy="19" r="1"/></svg></div></div>';
h+='<div class="ma" id="cms"></div><div class="typ" id="typ"><i></i><i></i><i></i><span>'+san(nm)+' typing</span></div>';
h+='<div class="atp" id="datp"></div>';
h+='<div class="ib"><button data-act="attach" data-ctx="dm"><svg viewBox="0 0 24 24"><path d="M12 5v14M5 12h14"/></svg></button>';
h+='<textarea id="din" placeholder="Message..." rows="1" data-oninp="dm"></textarea>';
h+='<button class="snd" id="dsnd" data-act="sendDM"><svg viewBox="0 0 24 24"><line x1="22" y1="2" x2="11" y2="13"/><polygon points="22 2 15 22 11 13 2 9 22 2"/></svg></button></div></div>';
el.innerHTML=h;rConvo();setTimeout(()=>{scr($('#cms'));if(opts?.focus!==false)$('#din')?.focus()},60);bindTextarea($('#din'),'dm')}

function rConvo(){const el=$('#cms');if(!el||!_cid)return;const c=S.co[_cid];if(!c){el.innerHTML='';return}
if(!c.m.length){el.innerHTML=emptyState('chat','Say hi 👋','Start the conversation');return}
let h='',ld='';c.m.forEach(m=>{if(m.hid)return;const d=df(m.ts);if(d!==ld){h+='<div class="md"><span class="mdp">'+d+'</span></div>';ld=d}
const me=m.fr===S.me?.id;h+='<div class="mr '+(me?'s':'r')+' ani" data-mid="'+m.id+'">';
if(m.rm){h+='<div class="mrm"><svg viewBox="0 0 24 24"><path d="M3 6h18M19 6v14a2 2 0 01-2 2H7a2 2 0 01-2-2V6"/></svg>Message removed</div>'}
else{h+='<div class="mb" data-m="'+m.id+'" data-longpress="msg">'+(m.hrt?'<span class="mheart">❤</span>':'');
if(m.rp)h+='<div class="mreply">'+san(m.rp)+'</div>';
if((m.mt==='image')&&m.med)h+='<div class="mimg" data-act="view" data-src="'+encodeURIComponent(m.med)+'" data-dbl="heart" data-mid="'+m.id+'"><img src="'+m.med+'"></div>';
else if(m.mt==='video'&&m.med)h+='<div class="mimg" data-act="view" data-src="'+encodeURIComponent(m.med)+'" data-vid="1"><video src="'+m.med+'" preload="metadata"></video></div>';
else h+='<div class="mt">'+lnk(san(m.txt||''))+'</div>';
if(m.fn&&m.med&&m.mt==='file')h+='<div class="matt" data-act="download" data-src="'+encodeURIComponent(m.med)+'" data-fn="'+san(m.fn)+'"><svg viewBox="0 0 24 24"><path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z"/><path d="M14 2v6h6"/></svg><div><div class="mfn">'+san(m.fn)+'</div><div class="mfs">'+fz(m.fs||0)+'</div></div></div>';
h+='<div class="mm">'+(m.ed?'<span class="med">edited</span>':'')+'<span class="mts">'+tf(m.ts)+'</span>'+(me?rcpt(m):'')+'</div></div>'}
h+='</div>'});el.innerHTML=h}

function rcpt(m){if(!S.cfg.rcpt)return'';const s=m.st||'sending';
if(s==='sending')return'<span class="rx"><i class="dot"></i></span>';
if(s==='sent')return'<span class="rx"><i class="chk"></i></span>';
if(s==='delivered')return'<span class="rx"><svg viewBox="0 0 24 24"><path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/></svg></span>';
if(s==='read')return'<span class="rx seen"><svg viewBox="0 0 24 24"><path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/></svg></span>';return''}

function rProfVw(opts){const pid=U._pid;if(!pid)return;const f=S.fr.find(f=>f.id===pid);const p=S.nb.find(p=>p.id===pid);const bot=S.bots?.find(b=>b.id===pid);
const info=p||f||bot||{id:pid,name:sid(pid)};const on=!!p;const la=S.cfg.la[pid];
const el=$('#views');
let h='<div class="'+(opts?.enter===false?'vw on':'vw on stack-enter')+'"><div class="tb"><button class="tbb" data-act="closeSub"><svg viewBox="0 0 24 24"><path d="M15 18l-6-6 6-6"/></svg></button><div class="tbc"><div class="tbt">'+san(info.name||sid(pid))+'</div></div><div class="tbb" data-act="openUserMenu" data-id="'+pid+'"><svg viewBox="0 0 24 24"><circle cx="12" cy="12" r="1"/><circle cx="12" cy="5" r="1"/><circle cx="12" cy="19" r="1"/></svg></div></div>';
h+='<div class="scr"><div class="pfw"><div class="pfavwrap"><div class="pfav">'+AV(info.av)+'</div></div>';
h+='<div class="pfnm">'+san(info.name||sid(pid))+'</div><div class="pfid" data-act="copyIdArg" data-id="'+pid+'">'+sid(pid)+'</div>';
h+='<div class="pfla">'+(on?'Online':la?'Active '+rt(la)+' ago':'Offline')+'</div>';
if(info.bio)h+='<div class="pfbio">'+san(info.bio)+'</div>';
if(info.lk)h+='<a class="pflk" href="'+san(info.lk)+'" target="_blank">'+san(info.lk.replace(/^https?:\/\//,'').slice(0,28))+'</a>';
if(info.sg)h+='<div class="pfsong" data-act="playPreview" data-song="'+encodeURIComponent(JSON.stringify(info.sg))+'"><div class="pfsa">'+(info.sg.art?'<img src="'+info.sg.art+'">':'')+'</div><div style="flex:1;min-width:0"><div class="pfst">'+san(info.sg.title)+'</div><div class="pfsar">'+san(info.sg.artist)+'</div></div></div>';
h+='<div class="pfacts">';
if(!bot)h+='<button class="btn bp" style="flex:1" data-act="openChat" data-id="'+pid+'">Message</button>';
h+='</div></div>';
const note=S.no.pe[pid];
if(note?.txt)h+='<div style="padding:0 16px;width:100%"><div class="sh" style="padding:8px 0">Note</div><div class="li" data-act="viewNote" data-id="'+pid+'" style="background:var(--s1);border-radius:var(--r);padding:9px"><div class="lb"><div class="ln" style="font-size:12px;font-weight:500">'+san(note.txt)+'</div></div></div></div>';
h+='</div></div>';el.innerHTML=h}

function rNoteVw(opts){const pid=U._nid;const n=pid==='me'?S.no.my:S.no.pe[pid];if(!n)return;
const nm=pid==='me'?S.me?.name:(n.name||sid(pid));const avUrl=pid==='me'?S.me?.av:n.av;
const el=$('#views');
let h='<div class="'+(opts?.enter===false?'vw on':'vw on stack-enter')+'"><div class="tb"><button class="tbb" data-act="closeSub"><svg viewBox="0 0 24 24"><path d="M15 18l-6-6 6-6"/></svg></button><div class="tbc"><div class="tbt">Note</div></div></div><div class="scr"><div class="nvw">';
h+='<div class="nva">'+AV(avUrl)+'</div>';
h+='<div class="nvn">'+san(nm||'')+'</div><div class="nvt">'+san(n.txt||'')+'</div>';
if(pid!=='me')h+='<div class="nvheart" data-act="likeNote" data-id="'+pid+'"><span>❤</span><em>'+(n.lk||0)+'</em></div>';
if(n.sg){h+='<div class="nvalb" id="nvalb"><img src="'+(n.sg.art||'')+'"></div>';
h+='<div class="nvst">'+san(n.sg.title)+'</div><div class="nvsa">'+san(n.sg.artist)+'</div>';
h+='<div class="nvpb"><div class="nvpf" id="nvpf"></div></div>';
h+='<div class="nvtt"><span id="nvc">0:00</span><span>0:30</span></div>';
h+='<div class="nvly" id="nvly"></div>'}
h+='</div></div></div>';el.innerHTML=h;
if(n.sg?.preview){const a=$('#aud');a.src=toHttps(n.sg.preview);a.volume=.6;
a.onerror=()=>console.log('Couldn\'t play this preview');
a.play().catch(()=>toast('Tap again to play preview'));
const al=$('#nvalb');if(al)al.classList.add('sp');
a.ontimeupdate=()=>{const p=$('#nvpf'),c=$('#nvc');if(p)p.style.width=((a.currentTime/a.duration)*100)+'%';
if(c){const s=a.currentTime|0;c.textContent=(s/60|0)+':'+String(s%60).padStart(2,'0')}};
a.onended=()=>{const x=$('#nvalb');if(x)x.classList.remove('sp')};
if(n.sg.title)fetchLyrics(n.sg.title,n.sg.artist||'')}}

async function fetchLyrics(t,ar){const el=$('#nvly');if(el)el.innerHTML='<div class="sgload"><div class="spin18"></div>Loading lyrics…</div>';
try{const r=await fetch('https://lrclib.net/api/search?track_name='+encodeURIComponent(t)+'&artist_name='+encodeURIComponent(ar));
if(!r.ok){if(el)el.innerHTML='';return}const d=await r.json();if(!d?.length){if(el)el.innerHTML='';return}const b=d[0];
if(b.syncedLyrics){const lines=[];b.syncedLyrics.split('\n').forEach(l=>{const m=l.match(/\[(\d+):(\d+\.\d+)\](.*)/);if(m)lines.push({t:parseInt(m[1])*60+parseFloat(m[2]),x:m[3].trim()})});
if(el){el.innerHTML=lines.map((l,i)=>'<div class="nvl" data-i="'+i+'">'+san(l.x)+'</div>').join('');
const upd=()=>{const a=$('#aud');if(!a||a.paused)return;let ai=-1;
for(let i=lines.length-1;i>=0;i--)if(a.currentTime>=lines[i].t){ai=i;break}
el.querySelectorAll('.nvl').forEach((e,i)=>e.classList.toggle('now',i===ai));
if(ai>=0)el.querySelector('[data-i="'+ai+'"]')?.scrollIntoView({block:'center',behavior:'smooth'});
requestAnimationFrame(upd)};requestAnimationFrame(upd)}}else if(el)el.innerHTML=''}catch(e){if(el)el.innerHTML=''}}

function rPips(){const el=$('#pips');if(!el)return;let h='';const has=S.no.my?.txt;
h+='<div class="npip" data-act="setNote"><div class="npawrap'+(has?' has':'')+'"><div class="npa">'+AV(S.me?.av)+'</div>'+(!has?'<div class="pp"><svg viewBox="0 0 24 24"><path d="M12 5v14M5 12h14"/></svg></div>':'')+'</div><div class="npn">You</div></div>';
Object.entries(S.no.pe).filter(([,n])=>n.txt).sort((a,b)=>b[1].ts-a[1].ts).forEach(([id,n])=>{
h+='<div class="npip" data-act="viewNote" data-id="'+id+'"><div class="npawrap has"><div class="npa">'+AV(n.av)+'</div></div><div class="npn">'+san((n.name||'').slice(0,6))+'</div></div>'});
el.innerHTML=h}

function updBdg(){let n=0;Object.values(S.co).forEach(c=>{n+=c.m.filter(m=>m.fr!==S.me?.id&&m.ts>(c.lr||0)&&!m.hid).length});
const d=$('#cdot');if(d)d.classList.toggle('on',n>0)}

function banner(t,m){$('#bAv').innerHTML=PERSON_SVG;$('#bTi').textContent=t;$('#bMs').textContent=m;
$('#ban').classList.add('on');clearTimeout(window._bt);window._bt=setTimeout(()=>$('#ban').classList.remove('on'),3200)}
function toast(msg){const wrap=$('#toastwrap');if(!wrap)return;const id='t'+(_toastN++);
const d=document.createElement('div');d.className='toast';d.id=id;
d.innerHTML='<svg viewBox="0 0 24 24"><path d="M20 6L9 17l-5-5"/></svg>'+san(msg);
wrap.appendChild(d);setTimeout(()=>{d.classList.add('out');setTimeout(()=>d.remove(),260)},1700)}

function bindSwipeBackDelegated(){
let sx=0,sy=0,tracking=false,root=null;
document.addEventListener('touchstart',e=>{if(!_sub)return;if(e.touches[0].clientX<28){root=$('#views');sx=e.touches[0].clientX;sy=e.touches[0].clientY;tracking=true}},{passive:true});
document.addEventListener('touchmove',e=>{if(!tracking||!root)return;const dx=e.touches[0].clientX-sx,dy=e.touches[0].clientY-sy;
if(dx>10&&Math.abs(dy)<40)root.style.transform='translateX('+Math.min(dx,120)+'px)'},{passive:true});
document.addEventListener('touchend',e=>{if(!tracking||!root)return;tracking=false;const dx=(e.changedTouches[0].clientX-sx);
root.style.transform='';if(dx>70){if(_sub==='c')U.closeC();else if(_sub)U.closeSub()}root=null},{passive:true});
}
function bindTextarea(el,ctx){if(!el)return;el.addEventListener('input',()=>U.oninp(el,ctx));
el.addEventListener('keydown',e=>{if(e.key==='Enter'&&!e.shiftKey){e.preventDefault();ctx==='pub'?U.txPub():U.txDM()}})}

window.U={
nav(v){if(_view===v&&!_sub)return;$('#aud').pause();$('#aud').src='';_view=v;_sub=null;_cid=null;_reply=null;_att={};
vib(3);
document.querySelectorAll('.tt').forEach(t=>t.classList.toggle('on',t.dataset.v===v));
rView({enter:'tab'})},
openC(pid){_cid=pid;_sub='c';if(!S.co[pid])S.co[pid]={m:[],lr:0};
if(!S.fr.find(f=>f.id===pid)){const p=S.nb.find(p=>p.id===pid)||S.bots?.find(b=>b.id===pid);
if(p){S.fr.push({id:pid,name:p.name,av:p.av,at:now()});save()}}
vib(4);rView({enter:'stack'});txRd(pid);updBdg()},
closeC(){_cid=null;_sub=null;_reply=null;_att={};rView({enter:false})},
viewP(pid){_sub='pr';U._pid=pid;vib(4);rView({enter:'stack'})},
viewN(pid){_sub='nv';U._nid=pid;vib(4);rView({enter:'stack'})},
closeSub(){$('#aud').pause();$('#aud').src='';_sub=null;rView({enter:false})},
pubSeg(seg){if(_pubSeg===seg)return;_pubSeg=seg;vib(3);rView({enter:false})},

txPub(){const inp=$('#pin');if(!inp)return;const txt=inp.value.trim();const att=_att.pub;
if(!txt&&!att)return;const mid=uid();
const m={t:'p',id:S.me.id,n:S.me.name,txt,mt:att?.mt||'text',med:att?.data,fn:att?.fn,fs:att?.fsize,mid,ts:now(),rp:_reply};
tx(m);pms.push(m);if(pms.length>350)pms.shift();markSeen(mid);chime('sent');vib(5);
netPush({id:mid,type:'pub',to:'public',from:S.me.id,fromName:S.me.name,fromAv:S.me.av,payload:m,ts:m.ts});
inp.value='';ag(inp);$('#psnd').classList.remove('on');_att.pub=null;_reply=null;$('#patp').classList.remove('on');$('#patp').innerHTML='';
rPub();scr($('#pms'))},

txDM(){const inp=$('#din');if(!inp)return;const txt=inp.value.trim();const pid=_cid;const att=_att.dm;
if((!txt&&!att)||!pid)return;const mid=uid();const mt=att?.mt||'text';
const m={id:mid,fr:S.me.id,txt,mt,med:att?.data,fn:att?.fn,fs:att?.fsize,ts:now(),st:'sent',rp:_reply};
S.co[pid].m.push(m);tx({t:'dm',id:S.me.id,to:pid,txt,mt,med:att?.data,fn:att?.fn,fs:att?.fsize,mid,ts:m.ts,rp:_reply});
markSeen(mid);save();chime('sent');vib(5);
netPush({id:mid,type:'dm',to:pid,from:S.me.id,fromName:S.me.name,fromAv:S.me.av,payload:m,ts:m.ts});
rConvo();rView({enter:false});inp.value='';ag(inp);$('#dsnd').classList.remove('on');_att.dm=null;_reply=null;
$('#datp').classList.remove('on');$('#datp').innerHTML='';scr($('#cms'));
setTimeout(()=>{const mg=S.co[pid]?.m.find(x=>x.id===mid);if(mg&&mg.st==='sent'){mg.st='delivered';save();rConvo()}},500)},

oninp(el,ctx){ag(el);const has=el.value.trim()||_att[ctx];
$('#'+(ctx==='pub'?'p':'d')+'snd')?.classList.toggle('on',!!has);
if(ctx==='dm'&&_cid)tx({t:'tp',id:S.me.id,to:_cid})},

mD(e,mid){_sx=e.clientX;_sy=e.clientY;_mid=mid;_mt=setTimeout(()=>{vib([6]);
const bub=document.querySelector('[data-mid="'+mid+'"] .mb');if(!bub)return;bub.classList.add('hi');
const c=S.co[_cid];const m=c?.m.find(x=>x.id===mid);if(!m||m.rm)return;
const me=m.fr===S.me?.id;const r=bub.getBoundingClientRect();
const bar=$('#ctxM');
let items='<div class="ctxreact">'+['❤','😂','👍','😮','😢','🔥'].map(em=>'<span data-act="react" data-mid="'+mid+'" data-em="'+em+'">'+em+'</span>').join('')+'</div>';
if(m.txt)items+='<div class="cx" data-act="ctxCopy" data-mid="'+mid+'"><svg viewBox="0 0 24 24"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 01-2-2V4a2 2 0 012-2h9a2 2 0 012 2v1"/></svg>Copy</div>';
items+='<div class="cx" data-act="ctxReply" data-mid="'+mid+'"><svg viewBox="0 0 24 24"><polyline points="9 17 4 12 9 7"/><path d="M20 18v-2a4 4 0 00-4-4H4"/></svg>Reply</div>';
if(me&&m.mt==='text')items+='<div class="cx" data-act="ctxEdit" data-mid="'+mid+'"><svg viewBox="0 0 24 24"><path d="M11 4H4a2 2 0 00-2 2v14a2 2 0 002 2h14a2 2 0 002-2v-7"/><path d="M18.5 2.5a2.121 2.121 0 013 3L12 15l-4 1 1-4 9.5-9.5z"/></svg>Edit</div>';
if(me)items+='<div class="cx dng" data-act="ctxUnsend" data-mid="'+mid+'"><svg viewBox="0 0 24 24"><path d="M3 6h18M19 6v14a2 2 0 01-2 2H7a2 2 0 01-2-2V6"/></svg>Unsend</div>';
items+='<div class="cx" data-act="ctxHide" data-mid="'+mid+'"><svg viewBox="0 0 24 24"><path d="M17.94 17.94A10.07 10.07 0 0112 20c-7 0-11-8-11-8a18.45 18.45 0 015.06-5.94"/><line x1="1" y1="1" x2="23" y2="23"/></svg>Hide</div>';
bar.innerHTML=items;const above=r.top>200;
bar.style.left=Math.max(4,Math.min(r.left,innerWidth-172))+'px';
bar.style.top=above?(r.top-4)+'px':(r.bottom+4)+'px';bar.style.transform=above?'translateY(-100%)':'none';
bar.classList.add('on');$('#ctxO').classList.add('on')},350)},
mU(){clearTimeout(_mt)},
mM(e){if(Math.abs(e.clientX-_sx)>40||Math.abs(e.clientY-_sy)>40){clearTimeout(_mt);
if(e.clientX-_sx>50&&Math.abs(e.clientY-_sy)<30&&_mid){const c=S.co[_cid];const m=c?.m.find(x=>x.id===_mid);
if(m&&!m.rm){vib(4);_reply=m.txt?.slice(0,36)||m.mt||'Media';
const el=$('#datp');if(el){el.classList.add('on');el.innerHTML='<div style="flex:1;border-left:2px solid var(--t1);padding-left:8px"><div style="font-size:10px;color:var(--t3)">Reply</div><div style="font-size:11px">'+san(_reply)+'</div></div><div class="ax" data-act="clearReply"><svg viewBox="0 0 24 24"><path d="M18 6L6 18M6 6l12 12"/></svg></div>'}}_mid=null}}},
clrRp(){_reply=null;const el=$('#datp')||$('#patp');if(el){el.classList.remove('on');el.innerHTML=''}},

react(mid,em){U.ctxOff();const c=S.co[_cid];if(!c)return;const m=c.m.find(x=>x.id===mid);if(!m)return;
m.hrt=em;save();rConvo();tx({t:'rx',id:S.me.id,to:_cid,mid,hrt:em});vib([6,20,6])},
dblHeart(mid,elc){const c=S.co[_cid];if(!c)return;const m=c.m.find(x=>x.id===mid);if(!m)return;
m.hrt='❤';save();rConvo();tx({t:'rx',id:S.me.id,to:_cid,mid,hrt:'❤'});vib([8,30,8]);
if(elc){const bh=document.createElement('div');bh.className='bigheart go';bh.textContent='❤';elc.style.position='relative';elc.appendChild(bh);setTimeout(()=>bh.remove(),820)}},

cxDo(a,mid){U.ctxOff();const c=S.co[_cid];if(!c)return;const m=c.m.find(x=>x.id===mid);if(!m)return;
if(a==='cp'){try{navigator.clipboard.writeText(m.txt||'')}catch(e){}toast('Copied')}
else if(a==='rp'){_reply=m.txt?.slice(0,36)||m.mt||'Media';
const el=$('#datp');if(el){el.classList.add('on');el.innerHTML='<div style="flex:1;border-left:2px solid var(--t1);padding-left:8px"><div style="font-size:10px;color:var(--t3)">Reply</div><div style="font-size:11px">'+san(_reply)+'</div></div><div class="ax" data-act="clearReply"><svg viewBox="0 0 24 24"><path d="M18 6L6 18M6 6l12 12"/></svg></div>'}$('#din')?.focus()}
else if(a==='ed'){U.sh('Edit Message','<textarea class="fi" id="edtx" rows="3" style="-webkit-user-select:text;user-select:text">'+san(m.txt)+'</textarea>',
'<button class="btn bg" style="flex:1" data-act="sheetClose">Cancel</button><button class="btn bp" style="flex:1" data-act="doEdit" data-mid="'+mid+'">Save</button>');setTimeout(()=>$('#edtx')?.focus(),200)}
else if(a==='us'){m.rm=1;m.txt='';m.med=null;save();
const row=document.querySelector('[data-mid="'+mid+'"]');if(row)row.classList.add('fadeout');
setTimeout(()=>{rConvo();rView({enter:false})},220);
tx({t:'us',id:S.me.id,to:_cid,mid})}
else if(a==='hi'){m.hid=1;save();rConvo()}},
doEd(mid){const t=$('#edtx')?.value?.trim();if(!t)return;const c=S.co[_cid];const m=c?.m.find(x=>x.id===mid);
if(m){m.txt=t;m.ed=1;save();rConvo();tx({t:'ed',id:S.me.id,to:_cid,mid,nt:t})}U.shOff()},
ctxOff(){$('#ctxM').classList.remove('on');$('#ctxO').classList.remove('on');document.querySelectorAll('.mb.hi').forEach(b=>b.classList.remove('hi'))},

ash(type,extra){const el=$('#ash');let h='<div class="pill"></div>';
if(type==='att'){const ctx=extra;
[['Photo','<rect x="3" y="3" width="18" height="18" rx="3"/><circle cx="8.5" cy="8.5" r="1.5"/><path d="M21 15l-5-5L5 21"/>','image'],
['Video','<rect x="2" y="4" width="16" height="16" rx="3"/><path d="M22 8l-4 3v2l4 3V8z"/>','video'],
['File','<path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z"/><path d="M14 2v6h6"/>','file']
].forEach(([l,ico,mt])=>{h+='<div class="ashi" data-act="pick" data-mt="'+mt+'" data-ctx="'+ctx+'"><svg viewBox="0 0 24 24">'+ico+'</svg><span>'+l+'</span></div>'})}
else if(type==='co'){
h+='<div class="ashi" data-act="viewProfile" data-id="'+_cid+'"><svg viewBox="0 0 24 24"><circle cx="12" cy="8" r="4"/><path d="M6 21v-2a4 4 0 014-4h4a4 4 0 014 4v2"/></svg><span>View Profile</span></div>';
h+='<div class="ashi" data-act="clearChat"><svg viewBox="0 0 24 24"><path d="M3 6h18M19 6v14a2 2 0 01-2 2H7a2 2 0 01-2-2V6"/></svg><span>Clear Chat</span></div>';
h+='<div class="ashi dng" data-act="blockUser" data-id="'+_cid+'"><svg viewBox="0 0 24 24"><circle cx="12" cy="12" r="10"/><path d="M4.93 4.93l14.14 14.14"/></svg><span>Block</span></div>'}
else if(type==='usr'){const pid=extra;
h+='<div class="ashi" data-act="openChat" data-id="'+pid+'"><svg viewBox="0 0 24 24"><path d="M21 15a2 2 0 01-2 2H7l-4 4V5a2 2 0 012-2h14a2 2 0 012 2z"/></svg><span>Message</span></div>';
h+='<div class="ashi" data-act="copyIdArg" data-id="'+pid+'"><svg viewBox="0 0 24 24"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 01-2-2V4a2 2 0 012-2h9a2 2 0 012 2v1"/></svg><span>Copy ID</span></div>';
h+='<div class="ashi dng" data-act="blockUser" data-id="'+pid+'"><svg viewBox="0 0 24 24"><circle cx="12" cy="12" r="10"/><path d="M4.93 4.93l14.14 14.14"/></svg><span>Block</span></div>'}
el.innerHTML=h;el.classList.add('on');vib(3)},
ashOff(){$('#ash').classList.remove('on')},

pick(mt,ctx){U.ashOff();const inp=$('#f'+(mt==='image'?'I':mt==='video'?'V':'F'));
inp.onchange=function(){const f=this.files[0];if(!f)return;
const reader=new FileReader();reader.onload=e=>{
_att[ctx]={mt,data:e.target.result,fn:f.name,fsize:f.size};
const el=$('#'+(ctx==='pub'?'p':'d')+'atp');
el.classList.add('on');const isImg=mt==='image';
el.innerHTML=(isImg?'<img src="'+e.target.result+'">':'<div style="width:38px;height:38px;border-radius:7px;background:var(--s2);display:flex;align-items:center;justify-content:center;flex-shrink:0"><svg viewBox="0 0 24 24" style="width:15px;height:15px;stroke:var(--t2);stroke-width:1.5;fill:none"><path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z"/></svg></div>')+'<div class="ai"><div class="an">'+san(f.name)+'</div><div class="as">'+fz(f.size)+'</div></div><div class="ax" data-act="clearAtt" data-ctx="'+ctx+'"><svg viewBox="0 0 24 24"><path d="M18 6L6 18M6 6l12 12"/></svg></div>';
$('#'+(ctx==='pub'?'p':'d')+'snd')?.classList.add('on')};
reader.readAsDataURL(f);this.value=''};inp.click()},
clrAtt(ctx){_att[ctx]=null;const el=$('#'+(ctx==='pub'?'p':'d')+'atp');el.classList.remove('on');el.innerHTML='';
const inp=$('#'+(ctx==='pub'?'p':'d')+'in');$('#'+(ctx==='pub'?'p':'d')+'snd')?.classList.toggle('on',!!inp?.value?.trim())},

accR(id){const r=S.rq.find(r=>r.id===id);if(!r)return;S.fr.push({id:r.id,name:r.name,av:r.av,at:now()});
if(!S.co[id])S.co[id]={m:[],lr:0};S.rq=S.rq.filter(r=>r.id!==id);save();rView();toast('Added '+r.name)},
decR(id){S.rq=S.rq.filter(r=>r.id!==id);save();rView()},
async doBlk(pid){const nm=S.fr.find(f=>f.id===pid)?.name||S.nb.find(p=>p.id===pid)?.name||sid(pid);
const ok=await U.cf('Block '+nm+'?','They will not be able to message you.','Block',1);if(!ok)return;
S.bl.push({id:pid,at:now()});S.fr=S.fr.filter(f=>f.id!==pid);delete S.co[pid];save();U.closeSub();rView();toast('Blocked')},

setNote(){U.sh('Set Note','<div class="flbl">Note</div><input class="fi" id="ntx" value="'+san(S.no.my?.txt||'')+'" maxlength="60" placeholder="What is on your mind?"><button class="btn bg" style="width:100%" data-act="pickSongFor" data-ctx="note">'+(S.no.my?.sg?'Change Song 🎵 '+san(S.no.my.sg.title):'+ Add a Song')+'</button>',
'<button class="btn bg" style="flex:1" data-act="clearNote">Clear</button><button class="btn bp" style="flex:1" data-act="saveNote">Save</button>');setTimeout(()=>$('#ntx')?.focus(),200)},
saveNote(){const t=$('#ntx')?.value?.trim();if(!t){U.shOff();return}
const nid=uid();S.no.my={id:nid,txt:t,sg:U._ns||S.no.my?.sg||null,ts:now()};delete U._ns;save();rPips?.();
tx({t:'nt',id:S.me.id,n:S.me.name,av:S.me.av,nt:t,sg:S.no.my.sg,ts:now(),nid});markSeen(nid);
netPush({id:nid,type:'note',to:'public',from:S.me.id,fromName:S.me.name,fromAv:S.me.av,payload:{txt:t,sg:S.no.my.sg,ts:now()},ts:now()});
U.shOff();toast('Note posted')},
clrNote(){S.no.my=null;save();rPips?.();U.shOff()},

pickSg(ctx){U.shOff();setTimeout(()=>{
U.sh('Search Songs','<div class="srch tight"><div class="srchbox"><svg class="sicon" viewBox="0 0 24 24"><circle cx="11" cy="11" r="8"/><path d="M21 21l-4.35-4.35"/></svg><input placeholder="Search a song..." id="sq" data-act="songQuery" data-ctx="'+ctx+'"></div></div><div id="sgr"></div>','');
setTimeout(()=>$('#sq')?.focus(),280)},280)},

async sqry(q,ctx){if(q.length<2){$('#sgr').innerHTML='';return}
$('#sgr').innerHTML=skRows(3);
const raw=await searchSongs(q);
const songs=raw.filter(s=>!!s.preview);
const el=$('#sgr');if(!el)return;
if(!songs.length){el.innerHTML='<div style="padding:18px;text-align:center;color:var(--t3)">No playable results</div>';return}
el.innerHTML='';songs.forEach((s)=>{const d=document.createElement('div');d.className='sgi';
d.innerHTML='<div class="sga">'+(s.album?.cover_small?'<img src="'+toHttps(s.album.cover_small)+'">':'')+'</div><div style="flex:1;min-width:0"><div class="sgt">'+san(s.title)+'</div><div class="sgar">'+san(s.artist?.name||'')+'</div></div><span class="lt">'+(s.duration/60|0)+':'+(s.duration%60).toString().padStart(2,'0')+'</span>';
d.onclick=()=>{const sg={title:s.title,artist:s.artist?.name||'',art:toHttps(s.album?.cover_medium||s.album?.cover_small||''),preview:toHttps(s.preview)};
if(ctx==='note'){U._ns=sg;U.shOff();setTimeout(()=>U.setNote(),250)}
else if(ctx==='prof'){S.me.song=sg;save();rView({enter:false});ann();U.shOff();toast('Song added')}};
el.appendChild(d)})},

editPr(){U.sh('Edit Profile','<div class="flbl">Name</div><input class="fi" id="epn" value="'+san(S.me.name)+'" maxlength="20"><div class="flbl">Bio</div><input class="fi" id="epb" value="'+san(S.me.bio||'')+'" maxlength="100" placeholder="About you..."><button class="btn bg" style="width:100%" data-act="pickAvatar">Choose Avatar</button><div id="avprev" style="margin-top:8px;text-align:center"></div>',
'<button class="btn bg" style="flex:1" data-act="sheetClose">Cancel</button><button class="btn bp" style="flex:1" data-act="saveProfile">Save</button>')},
pickAvatar(){$('#fI').onchange=function(){const f=this.files[0];if(!f)return;
$('#avprev').innerHTML='<div class="sgload"><div class="spin18"></div>Processing…</div>';
const r=new FileReader();r.onload=e=>{const i=new Image();i.onload=()=>{const c=document.createElement('canvas');const s=Math.min(i.width,i.height,220);c.width=s;c.height=s;
const ctx=c.getContext('2d');const off=[(i.width-s)/2,(i.height-s)/2];ctx.drawImage(i,off[0]<0?0:off[0],off[1]<0?0:off[1],s,s,0,0,s,s);
U._av=c.toDataURL('image/jpeg',.82);$('#avprev').innerHTML='<img src="'+U._av+'" style="width:64px;height:64px;border-radius:50%;object-fit:cover;border:2px solid var(--s4)">'};i.src=e.target.result};r.readAsDataURL(f)};$('#fI').click()},
savePr(){const n=$('#epn')?.value?.trim();if(!n||n.length<2){toast('Name too short');return}
S.me.name=n;S.me.bio=$('#epb')?.value?.trim()||'';if(U._av){S.me.av=U._av;delete U._av};save();rView({enter:false});ann();U.shOff();toast('Profile updated')},
setSong(){U.pickSg('prof')},
setLk(){U.sh('Add Link','<input class="fi" id="plk" value="'+(S.me.link||'')+'" placeholder="https://...">',
'<button class="btn bg" style="flex:1" data-act="sheetClose">Cancel</button><button class="btn bp" style="flex:1" data-act="saveLink">Save</button>');setTimeout(()=>$('#plk')?.focus(),200)},
saveLink(){S.me.link=$('#plk')?.value?.trim()||null;save();rView({enter:false});ann();U.shOff();toast('Link saved')},
priv(){U.sh('Privacy','<div class="si" data-act="togCfg" data-k="rcpt"><span>Read Receipts</span><div class="tog '+(S.cfg.rcpt?'on':'')+'"></div></div><div class="si" data-act="togCfg" data-k="snd"><span>Sound</span><div class="tog '+(S.cfg.snd?'on':'')+'"></div></div><div class="si" data-act="togCfg" data-k="vib"><span>Vibration</span><div class="tog '+(S.cfg.vib?'on':'')+'"></div></div>','')},
togCfg(k,elc){S.cfg[k]=S.cfg[k]?0:1;elc?.classList.toggle('on');save();vib(4)},
blkLst(){let h='';if(!S.bl.length)h='<div style="text-align:center;color:var(--t3);padding:18px">No blocked users</div>';
else S.bl.forEach(b=>{h+='<div class="si"><span>'+san(b.name||sid(b.id))+'</span><button class="btn bg" style="padding:5px 9px;font-size:11px" data-act="unblock" data-id="'+b.id+'">Unblock</button></div>'});
U.sh('Blocked',h,'')},
unblock(id){S.bl=S.bl.filter(x=>x.id!==id);save();U.blkLst()},
expD(){const d=JSON.stringify({me:S.me,fr:S.fr,co:S.co,no:S.no,bl:S.bl,feed:S.feed});const b=new Blob([d],{type:'application/json'});
const a=document.createElement('a');a.href=URL.createObjectURL(b);a.download='muji-backup.json';document.body.appendChild(a);a.click();a.remove();toast('Exported')},
impD(){$('#fF').onchange=function(){const f=this.files[0];if(!f)return;const r=new FileReader();
r.onload=e=>{try{const d=JSON.parse(e.target.result);if(d.me)S.me=d.me;if(d.fr)S.fr=d.fr;if(d.co)S.co=d.co;if(d.feed)S.feed=d.feed;
save();toast('Imported');rView({enter:false})}catch(e2){toast('Invalid file')}};r.readAsText(f);this.value=''};$('#fF').click()},
async clrD(){const ok=await U.cf('Clear All Data','This permanently deletes your profile, chats, and notes on this device.','Delete Everything',1);if(ok){localStorage.clear();location.reload()}},
async logout(){const ok=await U.cf('Log Out','You can log back in as '+san(S.me?.name||'you')+' anytime.','Log Out',0);if(!ok)return;
$('#app').classList.remove('on');vib(6);
const wb=$('#wb'),wc=$('#wbc');
wc.innerHTML='<div class="obwb"><div class="oav">'+AV(S.me.av)+'</div><div><b>'+san(S.me.name)+'</b><span>'+sid(S.me.id)+'</span></div></div><h2>See you soon</h2><p>Come back anytime — your chats and notes will be here.</p><button class="prim" data-act="continueSession">Continue as '+san(S.me.name)+'</button><button class="ghost" data-act="switchAccount">Switch Account</button>';
wb.classList.add('on')},
continueSession(){$('#wb').classList.remove('on');$('#app').classList.add('on');rView({enter:false})},
async switchAccount(){const ok=await U.cf('Switch Account','This deletes the current profile and all its data from this device.','Continue',1);if(!ok)return;
localStorage.clear();location.reload()},

addId(){U.sh('Add by ID','<input class="fi" id="aid" placeholder="Paste full peer ID">',
'<button class="btn bg" style="flex:1" data-act="sheetClose">Cancel</button><button class="btn bp" style="flex:1" data-act="doAdd">Find</button>');setTimeout(()=>$('#aid')?.focus(),200)},
doAdd(){const id=$('#aid')?.value?.trim();if(!id)return;const m=S.nb.find(p=>p.id===id||sid(p.id)===id);
if(m){U.shOff();U.viewP(m.id)}else toast('Not found nearby')},

likeNote(pid,elc){const n=S.no.pe[pid];if(!n)return;n.lk=(n.lk||0)+1;save();
if(elc){elc.classList.add('liked');elc.querySelector('em').textContent=n.lk;elc.style.transform='scale(1.15)';setTimeout(()=>elc.style.transform='',150)}
vib([8,20,8])},

likePost(id){const p=S.feed.find(x=>x.id===id);if(!p)return;
p.likedByMe=!p.likedByMe;p.likes+=p.likedByMe?1:-1;save();
const btn=document.querySelector('.postcard[data-postid="'+id+'"] .heartbtn');
if(btn)btn.classList.toggle('on',p.likedByMe);
const lc=$('#lc-'+id);if(lc)lc.textContent=p.likes;
vib([8,20,8])},

newPost(){U.sh('New Post','<div class="imgpickbox" id="npimg" data-act="pickPostImg"><svg viewBox="0 0 24 24"><rect x="3" y="3" width="18" height="18" rx="3"/><circle cx="8.5" cy="8.5" r="1.5"/><path d="M21 15l-5-5L5 21"/></svg><span>Choose a photo</span></div><textarea class="fi" id="npcap" rows="2" placeholder="Write a caption..." style="resize:none"></textarea>',
'<button class="btn bg" style="flex:1" data-act="sheetClose">Cancel</button><button class="btn bp" style="flex:1" data-act="submitPost">Post</button>')},
pickPostImg(){$('#fP').onchange=function(){const f=this.files[0];if(!f)return;const r=new FileReader();
r.onload=e=>{U._postImg=e.target.result;$('#npimg').innerHTML='<img src="'+e.target.result+'">'};r.readAsDataURL(f)};$('#fP').click()},
submitPost(){const cap=$('#npcap')?.value?.trim()||'';if(!U._postImg){toast('Pick a photo first');return}
const post={id:uid(),type:'user',authorId:S.me.id,authorName:S.me.name,authorAv:S.me.av,img:U._postImg,caption:cap,cat:'p2p',ts:now(),likes:0,likedByMe:false};
S.feed.unshift(post);S.feed=S.feed.slice(0,80);save();markSeen(post.id);
netPush({id:post.id,type:'post',to:'public',from:S.me.id,fromName:S.me.name,fromAv:S.me.av,payload:post,ts:post.ts});
delete U._postImg;U.shOff();toast('Posted');if(_pubSeg==='feed')rFeedList()},

async refreshFeed(btn){btn?.classList.add('spin');vib(4);await genBotFeedBatch();await genBotFeedBatch();btn?.classList.remove('spin');rFeedList();toast('Feed updated')},

playSg(){if(S.me?.song?.preview){const a=$('#aud');a.src=toHttps(S.me.song.preview);a.volume=.6;a.onerror=()=>toast('Couldn\'t play this preview');a.play().catch(()=>toast('Tap again to play'))}},
playPrvw(enc){try{const sg=JSON.parse(decodeURIComponent(enc));if(sg?.preview){const a=$('#aud');a.src=toHttps(sg.preview);a.volume=.6;a.onerror=()=>toast('Couldn\'t play this preview');a.play().catch(()=>toast('Tap again to play'))}}catch(e){}},

vwOn(src,isV){const el=$('#vwc');const d=typeof src==='string'&&src.startsWith('data:')?src:decodeURIComponent(src);
el.innerHTML=isV?'<video src="'+d+'" controls autoplay playsinline style="max-width:94%;max-height:78vh"></video>':'<img src="'+d+'" style="pointer-events:auto">';
$('#vwA').innerHTML='<button class="vwb" data-act="dlF" data-src="'+encodeURIComponent(d)+'">Save</button>';
$('#vwr').classList.add('on')},
vwOff(){$('#vwr').classList.remove('on');$('#vwc').innerHTML=''},
dl(src,fn){const a=document.createElement('a');a.href=decodeURIComponent(src);a.download=fn;document.body.appendChild(a);a.click();a.remove()},
dlF(src){const a=document.createElement('a');a.href=decodeURIComponent(src);a.download='muji-media';document.body.appendChild(a);a.click();a.remove();toast('Saved')},

sh(title,body,foot){const o=$('#shO');o.innerHTML='<div class="sheet"><div class="pill"></div><div class="shead"><span class="stitle">'+title+'</span><div class="sx" data-act="sheetClose"><svg viewBox="0 0 24 24"><path d="M18 6L6 18M6 6l12 12"/></svg></div></div><div class="sbody">'+body+'</div>'+(foot?'<div class="sfoot">'+foot+'</div>':'')+'</div>';o.classList.add('on')},
shOff(){const sheet=$('#shO .sheet');if(sheet){sheet.classList.add('closing');setTimeout(()=>$('#shO').classList.remove('on'),200)}else $('#shO').classList.remove('on')},

cf(t,m,ok='OK',dng=0){return new Promise(res=>{U._cfR=res;$('#cfH').textContent=t;$('#cfP').textContent=m;
$('#cfY').textContent=ok;$('#cfY').className='btn '+(dng?'bd':'bp');$('#cfm').classList.add('on')})},
cfR(v){$('#cfm').classList.remove('on');U._cfR?.(v)},

cp(t){try{navigator.clipboard.writeText(t)}catch(e){}toast('ID copied')},
fCh(q){q=q.toLowerCase();document.querySelectorAll('#chl .li').forEach(el=>{
const n=el.querySelector('.ln')?.textContent?.toLowerCase()||'';el.style.display=n.includes(q)?'':'none'})},
fPp(q){q=q.toLowerCase();if(q.length<2){rView({enter:false});return}
document.querySelectorAll('#ppl .li').forEach(el=>{const n=(el.querySelector('.ln')?.textContent||'').toLowerCase();
el.style.display=(n.includes(q)||el.querySelector('.ls')?.textContent?.toLowerCase()?.includes(q))?'':'none'})},
};

let _lastTapTime=0,_lastTapId=null;
document.addEventListener('click',e=>{
const t=e.target.closest('[data-act]');
if(!t)return;
if(t.dataset.stop)e.stopPropagation();
const a=t.dataset.act;
switch(a){
case 'attach':U.ash('att',t.dataset.ctx);break;
case 'sendPub':U.txPub();break;
case 'sendDM':U.txDM();break;
case 'openChat':U.openC(t.dataset.id);break;
case 'closeConvo':U.closeC();break;
case 'closeSub':U.closeSub();break;
case 'viewProfile':U.viewP(t.dataset.id);break;
case 'viewNote':U.viewN(t.dataset.id);break;
case 'openConvoMenu':U.ash('co');break;
case 'openUserMenu':U.ash('usr',t.dataset.id);break;
case 'acceptReq':U.accR(t.dataset.id);break;
case 'declineReq':U.decR(t.dataset.id);break;
case 'blockUser':U.doBlk(t.dataset.id);break;
case 'clearChat':U.ashOff();if(S.co[_cid]){S.co[_cid].m=[];save();rConvo()}break;
case 'view':U.vwOn(t.dataset.src,!!t.dataset.vid);break;
case 'download':U.dl(t.dataset.src,t.dataset.fn);break;
case 'dlF':U.dlF(t.dataset.src);break;
case 'pick':U.pick(t.dataset.mt,t.dataset.ctx);break;
case 'clearAtt':U.clrAtt(t.dataset.ctx);break;
case 'clearReply':U.clrRp();break;
case 'setNote':U.setNote();break;
case 'saveNote':U.saveNote();break;
case 'clearNote':U.clrNote();break;
case 'pickSongFor':U.pickSg(t.dataset.ctx);break;
case 'editProfile':U.editPr();break;
case 'pickAvatar':U.pickAvatar();break;
case 'saveProfile':U.savePr();break;
case 'setSong':U.setSong();break;
case 'setLink':U.setLk();break;
case 'saveLink':U.saveLink();break;
case 'privacy':U.priv();break;
case 'togCfg':U.togCfg(t.dataset.k,t.querySelector('.tog'));break;
case 'blockedList':U.blkLst();break;
case 'unblock':U.unblock(t.dataset.id);break;
case 'exportData':U.expD();break;
case 'importData':U.impD();break;
case 'clearData':U.clrD();break;
case 'logout':U.logout();break;
case 'continueSession':U.continueSession();break;
case 'switchAccount':U.switchAccount();break;
case 'addId':U.addId();break;
case 'doAdd':U.doAdd();break;
case 'copyId':U.cp(S.me.id);break;
case 'copyIdArg':U.cp(t.dataset.id);break;
case 'playMySong':U.playSg();break;
case 'playPreview':U.playPrvw(t.dataset.song);break;
case 'sheetClose':U.shOff();break;
case 'doEdit':U.doEd(t.dataset.mid);break;
case 'likeNote':U.likeNote(t.dataset.id,t);break;
case 'react':U.react(t.dataset.mid,t.dataset.em);break;
case 'ctxCopy':U.cxDo('cp',t.dataset.mid);break;
case 'ctxReply':U.cxDo('rp',t.dataset.mid);break;
case 'ctxEdit':U.cxDo('ed',t.dataset.mid);break;
case 'ctxUnsend':U.cxDo('us',t.dataset.mid);break;
case 'ctxHide':U.cxDo('hi',t.dataset.mid);break;
case 'pubSeg':U.pubSeg(t.dataset.seg);break;
case 'refreshFeed':U.refreshFeed(t);break;
case 'likePost':U.likePost(t.dataset.id);break;
case 'newPost':U.newPost();break;
case 'pickPostImg':U.pickPostImg();break;
case 'submitPost':U.submitPost();break;
}
});
document.addEventListener('input',e=>{
const t=e.target;
if(t.dataset?.act==='filterChats')U.fCh(t.value);
else if(t.dataset?.act==='filterPeople')U.fPp(t.value);
else if(t.dataset?.act==='songQuery')U.sqry(t.value,t.dataset.ctx);
});
document.addEventListener('dblclick',e=>{
const t=e.target.closest('[data-dbl="heart"]');
if(t){U.dblHeart(t.dataset.mid,t)}
});
document.addEventListener('touchend',e=>{
const t=e.target.closest('[data-dbl="heart"]');
if(!t)return;const tap=now();
if(_lastTapId===t.dataset.mid&&tap-_lastTapTime<320){U.dblHeart(t.dataset.mid,t);_lastTapTime=0;_lastTapId=null}
else{_lastTapTime=tap;_lastTapId=t.dataset.mid}
},{passive:true});
document.addEventListener('pointerdown',e=>{
const t=e.target.closest('[data-longpress="msg"]');
if(t)U.mD(e,t.dataset.m);
});
document.addEventListener('pointerup',()=>U.mU());
document.addEventListener('pointermove',e=>{if(_mt)U.mM(e)});
document.addEventListener('pointerleave',()=>U.mU());
$('#ctxO').addEventListener('click',()=>U.ctxOff());
$('#shO').addEventListener('click',e=>{if(e.target===e.currentTarget)U.shOff()});
$('#vwrClose').addEventListener('click',()=>U.vwOff());
$('#ban').addEventListener('click',()=>$('#ban').classList.remove('on'));
$('#cfN').addEventListener('click',()=>U.cfR(0));
$('#cfY').addEventListener('click',()=>U.cfR(1));
bindSwipeBackDelegated();

function rOb(step){const c=$('#obc');c.classList.remove('swipe');void c.offsetWidth;c.classList.add('swipe');
let dots='<div class="obdots"><div class="obdot '+(step===0?'on':'')+'"></div><div class="obdot '+(step===1?'on':'')+'"></div></div>';
if(step===0)c.innerHTML=dots+'<svg class="obmark" viewBox="0 0 60 60" fill="none"><path d="M8 42V18L18 32L28 18V42" stroke="white" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/><path d="M34 42V22C34 19.8 35.8 18 38 18H46C48.2 18 50 19.8 50 22V42" stroke="white" stroke-width="2" stroke-linecap="round"/></svg><h2>What should we call you?</h2><p>Pick a display name — you can change it anytime.</p><input id="obn" placeholder="Display name" maxlength="20" autocomplete="off" spellcheck="false"><button class="prim" id="obgo" disabled>Continue</button>';
else c.innerHTML=dots+'<h2>You are all set</h2><p>This is your private ID. Share it so people can add you directly.</p><div class="oid" id="obid">'+sid(S.me.id)+'<span class="cpTag">tap to copy</span></div><button class="prim" id="obgo">Enter MUJI</button>';
if(step===0){const inp=$('#obn'),btn=$('#obgo');inp.addEventListener('input',()=>{btn.disabled=inp.value.trim().length<2});inp.addEventListener('keydown',e=>{if(e.key==='Enter'&&!btn.disabled)obGo()});setTimeout(()=>inp.focus(),260)}
else{$('#obid').onclick=()=>{try{navigator.clipboard.writeText(S.me.id)}catch(e){}toast('ID copied')}}
$('#obgo').onclick=obGo}

window.obGo=()=>{if(!S.me){const n=$('#obn')?.value?.trim();if(!n||n.length<2)return;
S.me={id:uid(),name:n,bio:'',av:null,link:null,song:null};vib(6);rOb(1)}else{save();$('#ob').classList.remove('on');startApp()}};

function startApp(){$('#app').classList.add('on');U.nav('public');initBC();updBdg();seedPub();initBots();netPoll();
document.querySelectorAll('.tt').forEach(t=>t.onclick=()=>U.nav(t.dataset.v));
document.addEventListener('keydown',e=>{if(e.key==='Escape'){U.ctxOff();U.ashOff();U.shOff();U.vwOff();$('#cfm').classList.remove('on');$('#aud').pause();if(_sub)U.closeSub()}});
document.addEventListener('pointerdown',e=>{if($('#ash').classList.contains('on')&&!e.target.closest('.ash'))U.ashOff()});
document.addEventListener('focusin',e=>{if(e.target.matches('input,textarea'))setTimeout(()=>window.scrollTo(0,0),60)});
document.addEventListener('visibilitychange',()=>{if(!document.hidden){ann();netPoll();if(_cid)txRd(_cid)}});
setInterval(()=>{updBdg();ann()},5000);
setInterval(netPoll,6000);
setInterval(()=>genBotNotes(),1800000);
setInterval(()=>genBotFeedBatch(),1500000)}

window.addEventListener('pageshow',e=>{if(e.persisted)location.reload()});
history.scrollRestoration&&(history.scrollRestoration='manual');

document.addEventListener('contextmenu',e=>e.preventDefault());
load();
setTimeout(()=>{$('#boot').classList.add('out');setTimeout(()=>{$('#boot').style.display='none';
if(!S.me){$('#ob').classList.add('on');rOb(0)}else startApp()},700)},1600);
})();
