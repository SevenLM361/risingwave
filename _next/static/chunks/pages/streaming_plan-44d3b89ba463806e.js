(self.webpackChunk_N_E=self.webpackChunk_N_E||[]).push([[47],{64248:function(t,e,r){(window.__NEXT_P=window.__NEXT_P||[]).push(["/streaming_plan",function(){return r(830)}])},67670:function(t,e,r){"use strict";r.d(e,{_P:function(){return generatePointLinks},bK:function(){return layout},o:function(){return generateBoxLinks},rE:function(){return flipLayoutPoint}}),r(50361);var a=r(6162),n=r.n(a);function layout(t,e,r){let a=function(t){let e=new Map;for(let r of t)e.set(r.id,r);let r=new Map,a=new Map,getActorBoxNode=t=>{let n=a.get(t);if(void 0!==n)return n;let l={nextNodes:[]},o=e.get(t);if(void 0===o)throw Error("no such id ".concat(t));for(let t of o.parentIds)getActorBoxNode(t).nextNodes.push(l);return a.set(t,l),r.set(l,t),l};for(let e of t)getActorBoxNode(e.id);let n=new Map,l=[];for(let t of r.keys())l.push(t);for(let t of function(t){let e=[],r=[],a=new Map,visit=t=>{if(t.temp)throw Error("This is not a DAG");if(!t.perm){t.temp=!0;let r=-1;for(let e of t.node.nextNodes){a.get(e).isInput=!1,t.isOutput=!1;let n=visit(a.get(e));n>r&&(r=n)}t.temp=!1,t.perm=!0,t.g=r+1,e.unshift(t.node)}return t.g};for(let e of t){let t={node:e,temp:!1,perm:!1,isInput:!0,isOutput:!0,g:0};a.set(e,t),r.push(t)}let n=0;for(let t of r){let e=visit(t);e>n&&(n=e)}for(let t of r)t.g=n-t.g;let l=[];for(let t=0;t<n+1;++t)l.push({nodes:[],occupyRow:new Set});let o=new Map,i=new Map;for(let t of r)l[t.g].nodes.push(t.node),o.set(t.node,t.g);let putNodeInPosition=(t,e)=>{i.set(t,e),l[o.get(t)].occupyRow.add(e)},occupyLine=(t,e,r)=>{for(let a=t;a<=e;++a)l[a].occupyRow.add(r)},hasOccupied=(t,e)=>l[t].occupyRow.has(e),isStraightLineOccupied=(t,e,r)=>{if(r<0)return!1;for(let a=t;a<=e;++a)if(hasOccupied(a,r))return!0;return!1};for(let e of t)e.nextNodes.sort((t,e)=>o.get(e)-o.get(t));for(let t of l)for(let e of t.nodes){if(!i.has(e)){for(let t of e.nextNodes){if(i.has(t))continue;let r=-1;for(;isStraightLineOccupied(o.get(e),o.get(t),++r););putNodeInPosition(e,r),putNodeInPosition(t,r),occupyLine(o.get(e)+1,o.get(t)-1,r);break}if(!i.has(e)){let t=-1;for(;hasOccupied(o.get(e),++t););putNodeInPosition(e,t)}}for(let t of e.nextNodes){if(i.has(t))continue;let r=i.get(e);if(!isStraightLineOccupied(o.get(e)+1,o.get(t),r)){putNodeInPosition(t,r),occupyLine(o.get(e)+1,o.get(t)-1,r);continue}for(r=-1;isStraightLineOccupied(o.get(e)+1,o.get(t),++r););putNodeInPosition(t,r),occupyLine(o.get(e)+1,o.get(t)-1,r)}}let s=new Map;for(let e of t)s.set(e,[o.get(e),i.get(e)]);return s}(l)){let a=r.get(t[0]);if(!a)throw Error("no corresponding actorboxid of node ".concat(t[0]));let l=e.get(a);if(!l)throw Error("actorbox id ".concat(a," is not present in actorBoxIdToActorBox"));n.set(l,t[1])}return n}(t),l=new Map,o=new Map,i=0,s=0;for(let t of a){let e=t[0],r=t[1][0],a=t[1][1],c=l.get(r)||0;e.width>c&&l.set(r,e.width);let d=o.get(a)||0;e.height>d&&o.set(a,e.height),i=n()([r,i])||0,s=n()([a,s])||0}let c=new Map,d=new Map,getCumulativeMargin=(t,e,r,a)=>{let n=r.get(t);if(n)return n;if(0===t)n=0;else{let l=a.get(t-1);if(!l)throw Error("".concat(t-1," has no result"));n=getCumulativeMargin(t-1,e,r,a)+l+e}return r.set(t,n),n};for(let t=0;t<=i;++t)getCumulativeMargin(t,e,c,l);for(let t=0;t<=s;++t)getCumulativeMargin(t,r,d,o);let u=[];for(let[t,[e,r]]of a){let a=c.get(e),n=d.get(r);if(void 0!==a&&void 0!==n)u.push({id:t.id,x:a,y:n,data:t});else throw Error("x of layer ".concat(e,": ").concat(a,", y of row ").concat(r,": ").concat(n," "))}return u}function flipLayoutPoint(t,e,r,a){let n=function(t,e,r,a){let n=[];for(let{id:e,name:r,order:l,parentIds:o,...i}of t)n.push({id:e,name:r,parentIds:o,width:2*a,height:2*a,order:l,...i});let l=layout(n,e,r);return l.map(t=>{let{id:e,x:r,y:n,data:l}=t;return{id:e,data:l,x:r+a,y:n+a}})}(t,r,e,a);return n.map(t=>{let{id:e,x:r,y:a,data:n}=t;return{id:e,data:n,x:a,y:r}})}function generatePointLinks(t){let e=[],r=new Map;for(let e of t)r.set(e.id,e);for(let a of t)for(let t of a.data.parentIds){let n=r.get(t);e.push({points:[{x:a.x,y:a.y},{x:n.x,y:n.y}],source:a.id,target:t})}return e}function generateBoxLinks(t){let e=[],r=new Map;for(let e of t)r.set(e.id,e);for(let a of t)for(let t of a.data.parentIds){let n=r.get(t);e.push({points:[{x:a.x+a.data.width/2,y:a.y+a.data.height/2},{x:n.x+n.data.width/2,y:n.y+n.data.height/2}],source:a.id,target:t})}return e}},30707:function(t,e,r){"use strict";r.d(e,{Z:function(){return useFetch}});var a=r(66479),n=r(67294);function useFetch(t){let[e,r]=(0,n.useState)(),l=(0,a.pm)();return(0,n.useEffect)(()=>{let fetchData=async()=>{try{let e=await t();r(e)}catch(t){l({title:"Error Occurred",description:t.toString(),status:"error",duration:5e3,isClosable:!0}),console.error(t)}};fetchData()},[l,t]),{response:e}}},830:function(t,e,r){"use strict";r.r(e),r.d(e,{default:function(){return Streaming}});var a=r(85893),n=r(66479),l=r(40639),o=r(83234),i=r(20979),s=r(57026),c=r(47741),d=r(79855),u=r(97098),p=r(96486),f=r.n(p),h=r(9008),g=r.n(h),x=r(11163),m=r(67294),y=r(52189),v=r(50361),w=r.n(v);function DependencyGraph(t){let{mvDependency:e,svgWidth:r,selectedId:n,onSelectedIdChange:l}=t,o=(0,m.useRef)(),[i,s]=(0,m.useState)("0px"),c=(0,m.useCallback)(()=>{let t=(0,u.zx)().nodeSize([10,34,5]),r=w()(e),{width:a,height:n}=t(r);return{width:a,height:n,dag:r}},[e]),p=c();return(0,m.useEffect)(()=>{let{width:t,height:e,dag:a}=p,i=o.current,c=d.Ys(i),u=d.ak_,f=d.jvg().curve(u).x(t=>{let{x:e}=t;return e+10}).y(t=>{let{y:e}=t;return e}),isSelected=t=>t.data.id===n,h=c.select(".edges").selectAll(".edge").data(a.links()),applyEdge=t=>t.attr("d",t=>{let{points:e}=t;return f(e)}).attr("fill","none").attr("stroke-width",t=>isSelected(t.source)||isSelected(t.target)?2:1).attr("stroke",t=>isSelected(t.source)||isSelected(t.target)?y.rS.colors.teal["500"]:y.rS.colors.gray["300"]);h.exit().remove(),h.enter().call(t=>t.append("path").attr("class","edge").call(applyEdge)),h.call(applyEdge);let g=c.select(".nodes").selectAll(".node").data(a.descendants()),applyNode=t=>t.attr("transform",t=>{let{x:e,y:r}=t;return"translate(".concat(e+10,", ").concat(r,")")}).attr("fill",t=>isSelected(t)?y.rS.colors.teal["500"]:y.rS.colors.gray["500"]);g.exit().remove(),g.enter().call(t=>t.append("circle").attr("class","node").attr("r",5).call(applyNode)),g.call(applyNode);let x=c.select(".labels").selectAll(".label").data(a.descendants()),applyLabel=t=>t.text(t=>t.data.name).attr("x",r-10).attr("font-family","inherit").attr("text-anchor","end").attr("alignment-baseline","middle").attr("y",t=>t.y).attr("fill",t=>isSelected(t)?y.rS.colors.black["500"]:y.rS.colors.gray["500"]).attr("font-weight","600");x.exit().remove(),x.enter().call(t=>t.append("text").attr("class","label").call(applyLabel)),x.call(applyLabel);let m=c.select(".overlays").selectAll(".overlay").data(a.descendants()),applyOverlay=t=>t.attr("x",3).attr("height",24).attr("width",r-6).attr("y",t=>t.y-5-12+2+3).attr("rx",5).attr("fill",y.rS.colors.gray["500"]).attr("opacity",0).style("cursor","pointer");m.exit().remove(),m.enter().call(t=>t.append("rect").attr("class","overlay").call(applyOverlay).on("mouseover",function(t,e){d.Ys(this).transition().duration(parseInt(y.rS.transition.duration.normal)).attr("opacity",".10")}).on("mouseout",function(t,e){d.Ys(this).transition().duration(parseInt(y.rS.transition.duration.normal)).attr("opacity","0")}).on("mousedown",function(t,e){d.Ys(this).transition().duration(parseInt(y.rS.transition.duration.normal)).attr("opacity",".20")}).on("mouseup",function(t,e){d.Ys(this).transition().duration(parseInt(y.rS.transition.duration.normal)).attr("opacity",".10")}).on("click",function(t,e){l&&l(e.data.id)})),m.call(applyOverlay),s("".concat(e,"px"))},[e,n,r,l,p]),(0,a.jsxs)("svg",{ref:o,width:"".concat(r,"px"),height:i,children:[(0,a.jsx)("g",{className:"edges"}),(0,a.jsx)("g",{className:"nodes"}),(0,a.jsx)("g",{className:"labels"}),(0,a.jsx)("g",{className:"overlays"})]})}var j=r(39653),S=r(50471),k=r(63679),I=r(67670),b=r(36696),C=r(89734),N=r.n(C),A=r(34269);async function getActorBackPressures(){let t=await A.Z.get("/api/metrics/actor/back_pressures");return t}function calculatePercentile(t,e){let r=t.sort((t,e)=>t.value-e.value),a=Math.floor(r.length*e);return r[a].value}var _=r(21297),P=r(98032);function RateBar(t){let{samples:e}=t,r=(100*calculatePercentile(e,.5)).toFixed(6),n=(100*calculatePercentile(e,.95)).toFixed(6),o=(100*calculatePercentile(e,.99)).toFixed(6),i=100*calculatePercentile(e,.9),s=Math.ceil(i).toFixed(6)+"%",c="p50: ".concat(r,"% p95: ").concat(n,"% p99: ").concat(o,"%"),d=(0,P.H)("#C6F6D5").mix((0,P.H)("#C53030"),Math.ceil(i)).toHexString(),u="linear(to-r, ".concat("#C6F6D5",", ").concat(d,")");return(0,a.jsx)(_.u,{bgColor:"gray.100",label:c,children:(0,a.jsx)(l.xu,{width:s,bgGradient:u,children:(0,a.jsxs)(l.xv,{children:["p90: ",i.toFixed(6),"%"]})})})}function BackPressureTable(t){let{selectedFragmentIds:e}=t,[r,l]=(0,m.useState)(),o=(0,n.pm)();(0,m.useEffect)(()=>((async function(){for(;;)try{let t=await getActorBackPressures();t.outputBufferBlockingDuration=N()(t.outputBufferBlockingDuration,t=>(t.metric.fragment_id,t.metric.downstream_fragment_id)),l(t),await new Promise(t=>setTimeout(t,5e3))}catch(t){o({title:"Error Occurred",description:t.toString(),status:"error",duration:5e3,isClosable:!0}),console.error(t);break}})(),()=>{}),[o]);let isSelected=t=>e.has(t),i=(0,a.jsx)(b.xJ,{children:(0,a.jsxs)(b.iA,{variant:"simple",children:[(0,a.jsx)(b.Rn,{children:"Back Pressures (Last 30 minutes)"}),(0,a.jsx)(b.hr,{children:(0,a.jsxs)(b.Tr,{children:[(0,a.jsx)(b.Th,{children:"Fragment IDs → Downstream"}),(0,a.jsx)(b.Th,{children:"Block Rate"})]})}),(0,a.jsx)(b.p3,{children:r&&r.outputBufferBlockingDuration.filter(t=>isSelected(t.metric.fragment_id)).map(t=>(0,a.jsxs)(b.Tr,{children:[(0,a.jsx)(b.Td,{children:"Fragment ".concat(t.metric.fragment_id," -> ").concat(t.metric.downstream_fragment_id)}),(0,a.jsx)(b.Td,{children:(0,a.jsx)(RateBar,{samples:t.sample})})]},"".concat(t.metric.fragment_id,"_").concat(t.metric.downstream_fragment_id)))})]})});return(0,a.jsxs)(m.Fragment,{children:[(0,a.jsx)(g(),{children:(0,a.jsx)("title",{children:"Streaming Back Pressure"})}),i]})}let D=(0,k.ZP)(()=>r.e(171).then(r.t.bind(r,55171,23)));function FragmentGraph(t){let{planNodeDependencies:e,fragmentDependency:r,selectedFragmentId:n}=t,l=(0,m.useRef)(),{isOpen:o,onOpen:i,onClose:s}=(0,j.qY)(),[u,p]=(0,m.useState)(),f=(0,m.useCallback)(()=>t=>{p(t.data),i()},[i])(),h=(0,m.useCallback)(()=>{let t=w()(e),a=w()(r),n=new Map,l=new Set;for(let[e,r]of t){var o;let t=function(t,e){let{dx:r,dy:a}=e,n=d.G_s().nodeSize([a,r]),l=n(t);return l.each(t=>([t.x,t.y]=[t.y,t.x])),l.each(t=>t.x=-t.x),l}(r,{dx:72,dy:48}),{width:a,height:i}=function(t,e){let{margin:{top:r,bottom:a,left:n,right:l}}=e,o=1/0,i=-1/0,s=1/0,c=-1/0;return t.each(t=>i=t.x>i?t.x:i),t.each(t=>o=t.x<o?t.x:o),t.each(t=>c=t.y>c?t.y:c),t.each(t=>s=t.y<s?t.y:s),o-=n,i+=l,s-=r,c+=a,t.each(t=>t.x=t.x-o),t.each(t=>t.y=t.y-s),{width:i-o,height:c-s}}(t,{margin:{left:60,right:60,top:48,bottom:60}});n.set(e,{layoutRoot:t,width:a,height:i,extraInfo:"Actor ".concat(null===(o=r.data.actor_ids)||void 0===o?void 0:o.join(", "))||0}),l.add(e)}let i=(0,I.bK)(a.map(t=>{let{width:e,height:r,id:a,...l}=t,{width:o,height:i}=n.get(a);return{width:o,height:i,id:a,...l}}),60,60),s=new Map;i.forEach(t=>{let{id:e,x:r,y:a}=t;s.set(e,{x:r,y:a})});let c=[];for(let[t,e]of n){let{x:r,y:a}=s.get(t);c.push({id:t,x:r,y:a,...e})}let u=0,p=0;c.forEach(t=>{let{x:e,y:r,width:a,height:n}=t;p=Math.max(p,r+n+50),u=Math.max(u,e+a)});let f=(0,I.o)(i);return{layoutResult:c,fragmentLayout:i,svgWidth:u,svgHeight:p,links:f,includedFragmentIds:l}},[e,r]),{svgWidth:g,svgHeight:x,links:v,fragmentLayout:k,layoutResult:b,includedFragmentIds:C}=h();return(0,m.useEffect)(()=>{if(b){let t=l.current,e=d.Ys(t),r=d.h5h().x(t=>t.x).y(t=>t.y),isSelected=t=>t===n,applyActor=t=>{t.attr("transform",t=>{let{x:e,y:r}=t;return"translate(".concat(e,", ").concat(r,")")});let e=t.select(".actor-text-frag-id");e.empty()&&(e=t.append("text").attr("class","actor-text-frag-id")),e.attr("fill","black").text(t=>{let{id:e}=t;return"Fragment #".concat(e)}).attr("font-family","inherit").attr("text-anchor","end").attr("dy",t=>{let{height:e}=t;return e-12+12}).attr("dx",t=>{let{width:e}=t;return e-12}).attr("fill","black").attr("font-size",12);let a=t.select(".actor-text-actor-id");a.empty()&&(a=t.append("text").attr("class","actor-text-actor-id")),a.attr("fill","black").text(t=>{let{extraInfo:e}=t;return e}).attr("font-family","inherit").attr("text-anchor","end").attr("dy",t=>{let{height:e}=t;return e-12+24}).attr("dx",t=>{let{width:e}=t;return e-12}).attr("fill","black").attr("font-size",12);let n=t.select(".bounding-box");n.empty()&&(n=t.append("rect").attr("class","bounding-box")),n.attr("width",t=>{let{width:e}=t;return e-24}).attr("height",t=>{let{height:e}=t;return e-24}).attr("x",12).attr("y",12).attr("fill","white").attr("stroke-width",t=>{let{id:e}=t;return isSelected(e)?3:1}).attr("rx",5).attr("stroke",t=>{let{id:e}=t;return isSelected(e)?y.rS.colors.teal[500]:y.rS.colors.gray[500]});let l=t.select(".links");l.empty()&&(l=t.append("g").attr("class","links"));let applyLink=t=>t.attr("d",r),o=l.selectAll("path").data(t=>{let{layoutRoot:e}=t;return e.links()});o.enter().call(t=>(t.append("path").attr("fill","none").attr("stroke",y.rS.colors.gray[700]).attr("stroke-width",1.5).call(applyLink),t)),o.call(applyLink),o.exit().remove();let i=t.select(".nodes");i.empty()&&(i=t.append("g").attr("class","nodes"));let applyActorNode=t=>{t.attr("transform",t=>"translate(".concat(t.x,",").concat(t.y,")"));let e=t.select("circle");e.empty()&&(e=t.append("circle")),e.attr("fill",y.rS.colors.teal[500]).attr("r",12).style("cursor","pointer").on("click",(t,e)=>f(e));let r=t.select("text");return r.empty()&&(r=t.append("text")),r.attr("fill","black").text(t=>t.data.name).attr("font-family","inherit").attr("text-anchor","middle").attr("dy",21.6).attr("fill","black").attr("font-size",12).attr("transform","rotate(-8)"),t},s=i.selectAll(".actor-node").data(t=>{let{layoutRoot:e}=t;return e.descendants()});s.exit().remove(),s.enter().call(t=>t.append("g").attr("class","actor-node").call(applyActorNode)),s.call(applyActorNode)},a=e.select(".actors").selectAll(".actor").data(b);a.enter().call(t=>{let e=t.append("g").attr("class","actor").call(applyActor);return e}),a.call(applyActor),a.exit().remove();let o=e.select(".actor-links").selectAll(".actor-link").data(v),i=d.FdL,s=d.jvg().curve(i).x(t=>{let{x:e}=t;return e}).y(t=>{let{y:e}=t;return e}),applyEdge=t=>t.attr("d",t=>{let{points:e}=t;return s(e)}).attr("fill","none").attr("stroke-width",t=>isSelected(t.source)||isSelected(t.target)?2:1).attr("stroke",t=>isSelected(t.source)||isSelected(t.target)?y.rS.colors.teal["500"]:y.rS.colors.gray["300"]);o.enter().call(t=>t.append("path").attr("class","actor-link").call(applyEdge)),o.call(applyEdge),o.exit().remove()}},[b,v,n,f]),(0,a.jsxs)(m.Fragment,{children:[(0,a.jsxs)(S.u_,{isOpen:o,onClose:s,size:"5xl",children:[(0,a.jsx)(S.ZA,{}),(0,a.jsxs)(S.hz,{children:[(0,a.jsxs)(S.xB,{children:[null==u?void 0:u.operatorId," - ",null==u?void 0:u.name]}),(0,a.jsx)(S.ol,{}),(0,a.jsx)(S.fe,{children:o&&(null==u?void 0:u.node)&&(0,a.jsx)(D,{shouldCollapse:t=>{let{name:e}=t;return"input"===e||"fields"===e||"streamKey"===e},src:u.node,collapsed:3,name:null,displayDataTypes:!1})}),(0,a.jsx)(S.mz,{children:(0,a.jsx)(c.zx,{colorScheme:"blue",mr:3,onClick:s,children:"Close"})})]})]}),(0,a.jsxs)("svg",{ref:l,width:"".concat(g,"px"),height:"".concat(x,"px"),children:[(0,a.jsx)("g",{className:"actor-links"}),(0,a.jsx)("g",{className:"actors"})]}),(0,a.jsx)(BackPressureTable,{selectedFragmentIds:C})]})}var E=r(44527),F=r(30707),M=r(35413);function Streaming(){var t,e,r;let{response:p}=(0,F.Z)(M.gG),{response:h}=(0,F.Z)(M.jV),[y,v]=(0,m.useState)(),w=(0,x.useRouter)(),j=(0,m.useCallback)(()=>{if(h&&w.query.id){let t=parseInt(w.query.id),e=h.find(e=>e.tableId===t);if(e){let t=function(t){let e=[],r=new Map;for(let e in t.fragments){let a=t.fragments[e];for(let t of a.actors)r.set(t.actorId,t.fragmentId)}for(let a in t.fragments){let n=t.fragments[a],l=new Set;for(let t of n.actors)for(let e of t.upstreamActorId){let t=r.get(e);t&&l.add(t)}e.push({id:n.fragmentId.toString(),name:"Fragment ".concat(n.fragmentId),parentIds:Array.from(l).map(t=>t.toString()),width:0,height:0,order:n.fragmentId,fragment:n})}return e}(e);return{fragments:e,fragmentDep:t,fragmentDepDag:(0,u.lu)()(t)}}}},[h,w.query.id]);(0,m.useEffect)(()=>(p&&!w.query.id&&p.length>0&&w.replace("?id=".concat(p[0].id)),()=>{}),[w,w.query.id,p]);let S=null===(t=j())||void 0===t?void 0:t.fragmentDep,k=null===(e=j())||void 0===e?void 0:e.fragmentDepDag,I=null===(r=j())||void 0===r?void 0:r.fragments,b=(0,m.useCallback)(()=>{let t=null==I?void 0:I.fragments;if(t){let e=new Map;for(let r in t){let a=t[r],n=function(t){let e=t.actors[0],hierarchyActorNode=t=>{var e;return{name:(null===(e=t.nodeBody)||void 0===e?void 0:e.$case.toString())||"unknown",children:(t.input||[]).map(hierarchyActorNode),operatorId:t.operatorId,node:t}},r="noDispatcher";e.dispatcher.length>1?r=e.dispatcher.every(t=>t.type===e.dispatcher[0].type)?"".concat(f().camelCase(e.dispatcher[0].type),"Dispatchers"):"multipleDispatchers":1===e.dispatcher.length&&(r="".concat(f().camelCase(e.dispatcher[0].type),"Dispatcher"));let a=t.actors.reduce((t,e)=>(t[e.actorId]=e.dispatcher,t),{});return d.bT9({name:r,actor_ids:t.actors.map(t=>t.actorId.toString()),children:e.nodes?[hierarchyActorNode(e.nodes)]:[],operatorId:"dispatcher",node:a})}(a);e.set(r,n)}return e}},[null==I?void 0:I.fragments]),C=b(),N=(0,m.useCallback)(()=>{let t=w.query.id;if(t&&p)return p.find(e=>e.id==parseInt(t))},[p,w.query.id]),A=N(),[_,P]=(0,m.useState)(""),[D,L]=(0,m.useState)(""),setRelationId=t=>w.replace("?id=".concat(t)),B=(0,n.pm)(),handleSearchFragment=()=>{let t=parseInt(D);if(h){for(let e of h)for(let r in e.fragments)if(e.fragments[r].fragmentId==t){setRelationId(e.tableId),v(t);return}}B({title:"Fragment not found",description:"",status:"error",duration:5e3,isClosable:!0})},handleSearchActor=()=>{let t=parseInt(_);if(h)for(let e of h)for(let r in e.fragments){let a=e.fragments[r];for(let r of a.actors)if(r.actorId==t){setRelationId(e.tableId),v(a.fragmentId);return}}B({title:"Actor not found",description:"",status:"error",duration:5e3,isClosable:!0})},R=(0,a.jsxs)(l.kC,{p:3,height:"calc(100vh - 20px)",flexDirection:"column",children:[(0,a.jsx)(E.Z,{children:"Streaming Plan"}),(0,a.jsxs)(l.kC,{flexDirection:"row",height:"full",width:"full",children:[(0,a.jsxs)(l.gC,{mr:3,spacing:3,alignItems:"flex-start",width:200,height:"full",children:[(0,a.jsxs)(o.NI,{children:[(0,a.jsx)(o.lX,{children:"Relations"}),(0,a.jsx)(i.II,{list:"relationList",spellCheck:!1,onChange:t=>{var e;let r=null==p?void 0:null===(e=p.find(e=>e.name==t.target.value))||void 0===e?void 0:e.id;r&&setRelationId(r)},placeholder:"Search...",mb:2}),(0,a.jsx)("datalist",{id:"relationList",children:p&&p.map(t=>(0,a.jsxs)("option",{value:t.name,children:["(",t.id,") ",t.name]},t.id))}),(0,a.jsx)(s.Ph,{value:w.query.id,onChange:t=>setRelationId(parseInt(t.target.value)),children:p&&p.map(t=>(0,a.jsxs)("option",{value:t.id,children:["(",t.id,") ",t.name]},t.name))})]}),(0,a.jsxs)(o.NI,{children:[(0,a.jsx)(o.lX,{children:"Goto"}),(0,a.jsxs)(l.gC,{spacing:2,children:[(0,a.jsxs)(l.Ug,{children:[(0,a.jsx)(i.II,{placeholder:"Fragment Id",value:D,onChange:t=>L(t.target.value)}),(0,a.jsx)(c.zx,{onClick:t=>handleSearchFragment(),children:"Go"})]}),(0,a.jsxs)(l.Ug,{children:[(0,a.jsx)(i.II,{placeholder:"Actor Id",value:_,onChange:t=>P(t.target.value)}),(0,a.jsx)(c.zx,{onClick:t=>handleSearchActor(),children:"Go"})]})]})]}),(0,a.jsxs)(l.kC,{height:"full",width:"full",flexDirection:"column",children:[(0,a.jsx)(l.xv,{fontWeight:"semibold",children:"Plan"}),A&&(0,a.jsxs)(l.xv,{children:[A.id," - ",A.name]}),k&&(0,a.jsx)(l.xu,{flex:"1",overflowY:"scroll",children:(0,a.jsx)(DependencyGraph,{svgWidth:200,mvDependency:k,onSelectedIdChange:t=>v(parseInt(t)),selectedId:null==y?void 0:y.toString()})})]})]}),(0,a.jsxs)(l.xu,{flex:1,height:"full",ml:3,overflowX:"scroll",overflowY:"scroll",children:[(0,a.jsx)(l.xv,{fontWeight:"semibold",children:"Fragment Graph"}),C&&S&&(0,a.jsx)(FragmentGraph,{selectedFragmentId:null==y?void 0:y.toString(),fragmentDependency:S,planNodeDependencies:C})]})]})]});return(0,a.jsxs)(m.Fragment,{children:[(0,a.jsx)(g(),{children:(0,a.jsx)("title",{children:"Streaming Fragments"})}),R]})}}},function(t){t.O(0,[662,184,476,155,591,323,855,340,688,383,413,774,888,179],function(){return t(t.s=64248)}),_N_E=t.O()}]);