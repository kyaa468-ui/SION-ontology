'use strict';
const titles = {issue:'조사 질문', change:'수정안', application:'적용', observation_job:'실제 사용과 효과'};
const fields = {question:'질문',occurrence_count:'발생 수',target_id:'수정 대상',goal:'목표',
  uses:'실제 사용',recurrences:'재발',side_effects:'부작용',missing:'자료 누락'};
async function render() {
  const status = document.getElementById('budget');
  try {
    const response = await fetch('/api/improvement', {cache:'no-store'});
    if (!response.ok) throw new Error('조회 실패');
    const data = await response.json();
    const b = data.budget;
    status.textContent = `${b.day} 한국 시간 · 연구 비용 $${b.actual_usd.toFixed(2)} / $5.00 · 예약 $${b.reserved_usd.toFixed(2)} · 미정산 ${b.unresolved_calls}건`;
    if (data.capacity?.state === 'configuration_pending') status.textContent += ' · 저장 예산 설정 대기: 새 AI 연구·검증 사본 생성 보류';
    const main = document.getElementById('improvements');
    main.replaceChildren();
    for (const [kind, rows] of Object.entries(data.sections)) {
      const section = document.createElement('section');
      const h = document.createElement('h2'); h.textContent = titles[kind]; section.append(h);
      if (!rows.length) { const p=document.createElement('p'); p.textContent='기록 없음'; section.append(p); }
      for (const row of rows) {
        const article = document.createElement('article');
        const heading = document.createElement('h3'); heading.textContent=data.state_labels[row.state] || row.state; article.append(heading);
        for (const [key, label] of Object.entries(fields)) {
          if (row[key] === undefined) continue;
          const p=document.createElement('p'); p.textContent=`${label}: ${row[key]}`; article.append(p);
        }
        const details=document.createElement('details');const summary=document.createElement('summary');summary.textContent='연결 정보';
        const pre=document.createElement('pre');pre.style.whiteSpace='pre-wrap';pre.style.overflowWrap='anywhere';pre.textContent=JSON.stringify(row,null,2);
        details.append(summary,pre);article.append(details);section.append(article);
      }
      main.append(section);
    }
  } catch (_) { status.textContent='개선 기록을 읽을 수 없습니다. 기록 없음과 조회 오류를 구분하여 다시 확인하십시오.'; }
}
render();
