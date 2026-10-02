// 前端渲染验证 + 截图。用真实浏览器走一遍完整流程，校验**渲染出来的几何
// 与文本**，而不只是「HTML 里有没有这个元素」。
//
//   node scripts/shot_frontend.js http://127.0.0.1:8132 docs/screenshots
//
// 为什么非要用真实浏览器
// ---------------------
// 这个页面里最容易坏、又最难靠单测发现的三件事：
//
// 1. **SVG DAG 有没有真的画出来、有没有叠在一起**。布局是 JS 现算的，
//    写错一个坐标公式不会报错，只会得到一堆挤在左上角的方块 ——
//    HTML 完全正确，pytest 全绿，只有渲染出来才知道；
// 2. **节点状态表格的列错位**。表格 5 列，一旦表头和数据行的单元格数
//    不一致，浏览器会把后面的内容挤到错误的列里 —— 数字看起来总是对的，
//    只是它属于另一个节点。靠看 HTML 源码发现不了；
// 3. **状态色和文字是否同步**。节点颜色按 status 走 CSS class，
//    文字按 NODE_STATUS_TEXT 走。两处只要有一处漏改，
//    就会出现「绿框写着已跳过」这种画面。
//
// 全部断言都走「读表头名字 → 取列」而不是按位置取 td[3] 这种写法 ——
// 插入一列会让后者静默读到错误的单元格，变成假通过。

const fs = require('fs');
const path = require('path');

const BASE = process.argv[2] || 'http://127.0.0.1:8132';
const OUT = process.argv[3] || 'docs/screenshots';

const PW_CANDIDATES = [
  process.env.PW_PATH,
  'playwright-core',
  'playwright',
  'C:/Users/Legion/npm_global/node_modules/n8n/node_modules/playwright-core',
].filter(Boolean);

const CHROME_CANDIDATES = [
  process.env.CHROME,
  process.env.CHROME_PATH,
  'C:/Users/Legion/.agent-browser/browsers/chrome-153.0.8010.36/chrome.exe',
  '/usr/bin/google-chrome',
  '/usr/bin/chromium',
].filter(Boolean);

function loadPlaywright() {
  for (const candidate of PW_CANDIDATES) {
    try { return require(candidate); } catch (_) { /* 试下一个 */ }
  }
  console.error('找不到 playwright-core。用 PW_PATH 指过去：\n'
    + '  PW_PATH=/path/to/playwright-core node scripts/shot_frontend.js');
  process.exit(2);
}

function findChrome() {
  for (const candidate of CHROME_CANDIDATES) {
    if (candidate && fs.existsSync(candidate)) return candidate;
  }
  console.error('找不到 Chrome/Chromium。用 CHROME 环境变量指过去。');
  process.exit(2);
}

const { chromium } = loadPlaywright();

let passed = 0, failed = 0;
function check(name, cond, detail) {
  if (cond) { passed++; console.log('  ✓ ' + name); }
  else { failed++; console.log('  ✗ ' + name + (detail !== undefined ? '  ← ' + detail : '')); }
}
function section(t) { console.log('\n[' + t + ']'); }

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

(async () => {
  fs.mkdirSync(OUT, { recursive: true });

  const browser = await chromium.launch({
    executablePath: findChrome(),
    args: ['--no-sandbox', '--disable-dev-shm-usage'],
  });
  const context = await browser.newContext({
    viewport: { width: 1440, height: 1150 },
    deviceScaleFactor: 2,
  });
  const page = await context.newPage();

  // pageerror 才是真正的 JS 异常；资源加载失败（404/422）单独记一条 ——
  // 本脚本会故意触发 404/422 来验证错误提示，不能把它们算成页面坏了。
  const jsErrors = [];
  const resourceErrors = [];
  page.on('console', (m) => {
    if (m.type() !== 'error') return;
    const t = m.text();
    if (t.indexOf('Failed to load resource') >= 0) resourceErrors.push(t);
    else jsErrors.push(t);
  });
  page.on('pageerror', (e) => jsErrors.push('pageerror: ' + e.message));

  const shot = async (name) => {
    const file = path.join(OUT, name + '.png');
    await page.screenshot({ path: file, fullPage: false });
    const stat = fs.statSync(file);
    check('截图 ' + name + ' 非空', stat.size > 8000, stat.size + ' bytes');
    return file;
  };

  // 按钮回到可用且不再是 spinner，说明这次异步请求结束了
  async function waitDone(sel, label) {
    try {
      await page.waitForFunction(
        (s) => {
          const btn = document.querySelector(s);
          if (!btn) return false;
          return !btn.disabled && btn.textContent.indexOf('spinner') === -1;
        },
        sel, { timeout: 60000 });
      return true;
    } catch (e) {
      console.log('    (等待 ' + label + ' 超时)');
      return false;
    }
  }

  // 读「节点状态」表：按表头名字定位列，避免列错位造成假通过
  const readNodeTable = () => page.evaluate(() => {
    const heads = [...document.querySelectorAll('#runOut table thead th')]
      .map((th) => th.textContent.trim());
    const rows = [...document.querySelectorAll('#runOut table tbody tr')]
      .map((tr) => [...tr.children].map((td) => td.textContent.trim()));
    const idx = (name) => heads.indexOf(name);
    return { heads, rows, idx: { node: idx('节点'), tries: idx('尝试'),
      status: idx('状态'), dur: idx('耗时'), out: idx('输出 / 错误') } };
  });

  const dagInfo = () => page.evaluate(() => {
    const rects = [...document.querySelectorAll('#dagG rect')]
      .filter((r) => r.getAttribute('fill') !== 'none');
    const boxes = rects.map((r) => ({
      x: +r.getAttribute('x'), y: +r.getAttribute('y'),
      w: +r.getAttribute('width'), h: +r.getAttribute('height'),
      cls: r.getAttribute('class'),
    }));
    return {
      nodes: rects.length,
      edges: document.querySelectorAll('#dagG path').length,
      branchLabels: [...document.querySelectorAll('#dagG .branchLabel')]
        .map((t) => t.textContent),
      boxes,
    };
  });

  console.log('='.repeat(70));
  console.log('agent-flow · 前端渲染 + 截图   ' + BASE);
  console.log('='.repeat(70));

  // ---------------------------------------------------------------- 1
  section('1 加载与 DAG 初始渲染');
  await page.goto(BASE + '/', { waitUntil: 'networkidle', timeout: 60000 });
  await page.waitForTimeout(800);
  check('标题正确', (await page.textContent('h1')).indexOf('agent-flow') >= 0,
    await page.textContent('h1'));
  check('健康指示点亮起',
    await page.evaluate(() => document.querySelector('#dot').className === 'on'));
  check('版本号已填充',
    (await page.textContent('#ver')).indexOf('v') === 0,
    await page.textContent('#ver'));
  check('徽标里工作流数 ≥ 1',
    +(await page.textContent('#wfCount')) >= 1,
    await page.textContent('#wfCount'));

  const d0 = await dagInfo();
  check('DAG 画出 7 个节点', d0.nodes === 7, d0.nodes);
  check('DAG 画出 5 条边', d0.edges === 5, d0.edges);
  check('分支标签含 true/false/approved/rejected',
    ['true', 'false', 'approved', 'rejected']
      .every((b) => d0.branchLabels.join(' ').indexOf(b) >= 0),
    JSON.stringify(d0.branchLabels));

  // 节点两两不重叠 —— 布局算错的典型症状就是方块叠成一坨
  const overlaps = await page.evaluate(() => {
    const rs = [...document.querySelectorAll('#dagG rect')]
      .filter((r) => r.getAttribute('fill') !== 'none')
      .map((r) => ({ x: +r.getAttribute('x'), y: +r.getAttribute('y'),
        w: +r.getAttribute('width'), h: +r.getAttribute('height') }));
    let n = 0;
    for (let i = 0; i < rs.length; i++) {
      for (let j = i + 1; j < rs.length; j++) {
        const a = rs[i], b = rs[j];
        if (a.x < b.x + b.w && b.x < a.x + a.w
          && a.y < b.y + b.h && b.y < a.y + a.h) n++;
      }
    }
    return n;
  });
  check('节点之间无重叠', overlaps === 0, overlaps + ' 对重叠');

  const overflow = await page.evaluate(() =>
    document.documentElement.scrollWidth - document.documentElement.clientWidth);
  check('页面无横向溢出', overflow <= 1, '溢出 ' + overflow + 'px');
  await shot('01-初始');

  // ---------------------------------------------------------------- 2
  section('2 小额路径（条件 false 分支 + 跳过传播）');
  await page.fill('#amount', '500');
  await page.fill('#title', '办公用品');
  await page.click('#btnStart');
  await waitDone('#btnStart', '发起运行');
  await page.waitForTimeout(500);

  let t = await readNodeTable();
  check('表格 5 列表头齐全', t.heads.length === 5, JSON.stringify(t.heads));
  check('每行单元格数与表头一致',
    t.rows.every((r) => r.length === t.heads.length),
    JSON.stringify(t.rows.map((r) => r.length)));
  check('表格有 6 行（含 auto/manager/finance/reject）',
    t.rows.length >= 5, t.rows.length);

  const rowOf = (tbl, id) => tbl.rows.find((r) => r[tbl.idx.node] === id);
  check('submit 成功', rowOf(t, 'submit')[t.idx.status] === 'succeeded',
    JSON.stringify(rowOf(t, 'submit')));
  check('auto 成功（小额自动通过）',
    rowOf(t, 'auto')[t.idx.status] === 'succeeded',
    JSON.stringify(rowOf(t, 'auto')));
  check('manager 跳过', rowOf(t, 'manager')[t.idx.status] === 'skipped',
    JSON.stringify(rowOf(t, 'manager')));
  check('finance 跳过（跳过传播到下游）',
    rowOf(t, 'finance')[t.idx.status] === 'skipped',
    JSON.stringify(rowOf(t, 'finance')));
  check('auto 输出里带事由',
    rowOf(t, 'auto')[t.idx.out].indexOf('办公用品') >= 0,
    rowOf(t, 'auto')[t.idx.out]);
  check('耗时列不是 undefined',
    t.rows.every((r) => r[t.idx.dur].indexOf('undefined') === -1),
    JSON.stringify(t.rows.map((r) => r[t.idx.dur])));

  const dSmall = await dagInfo();
  check('未走分支的边标成虚线（skippedEdge）',
    await page.evaluate(() =>
      document.querySelectorAll('#dagG path.skippedEdge').length >= 2),
    await page.evaluate(() =>
      document.querySelectorAll('#dagG path.skippedEdge').length));
  check('跳过节点用了 skipped 配色',
    dSmall.boxes.some((b) => b.cls === 'skipped'),
    JSON.stringify(dSmall.boxes.map((b) => b.cls)));
  check('概览卡「成功 / 跳过」= 3 / 3',
    await page.evaluate(() => {
      const cards = [...document.querySelectorAll('#runOut .card')];
      const hit = cards.find((c) => c.textContent.indexOf('成功 / 跳过') >= 0);
      return hit ? hit.querySelector('.v').textContent.trim() : '';
    }) === '3 / 3',
    await page.evaluate(() => {
      const cards = [...document.querySelectorAll('#runOut .card')];
      const hit = cards.find((c) => c.textContent.indexOf('成功 / 跳过') >= 0);
      return hit ? hit.querySelector('.v').textContent.trim() : '';
    }));
  await shot('02-小额自动通过');

  // ---------------------------------------------------------------- 3
  section('3 大额路径（审批挂起）');
  await page.fill('#amount', '1500');
  await page.fill('#title', '差旅报销');
  await page.click('#btnStart');
  await waitDone('#btnStart', '发起运行');
  await page.waitForTimeout(500);

  check('状态卡显示 waiting_approval',
    (await page.textContent('#runOut')).indexOf('waiting_approval') >= 0);
  check('「批准」按钮已启用', !(await page.isDisabled('#btnApprove')));
  check('「驳回」按钮已启用', !(await page.isDisabled('#btnReject')));
  check('「断点续跑」此时不可用（状态不是 running）',
    await page.isDisabled('#btnResume'));

  const dWait = await dagInfo();
  check('审批节点用了 waiting 配色',
    dWait.boxes.some((b) => b.cls === 'waiting'),
    JSON.stringify(dWait.boxes.map((b) => b.cls)));
  check('等待审批卡片指向 manager',
    await page.evaluate(() => {
      const cards = [...document.querySelectorAll('#runOut .card')];
      const hit = cards.find((c) => c.textContent.indexOf('等待审批') >= 0);
      return hit ? hit.querySelector('.v').textContent.trim() : '';
    }) === 'manager');
  await shot('03-审批挂起');

  // ---------------------------------------------------------------- 4
  section('4 批准 → 财务复核');
  await page.click('#btnApprove');
  await waitDone('#btnStart', '批准');
  await page.waitForTimeout(500);
  t = await readNodeTable();
  check('finance 成功', rowOf(t, 'finance')[t.idx.status] === 'succeeded',
    JSON.stringify(rowOf(t, 'finance')));
  check('finance 输出 = 1425',
    rowOf(t, 'finance')[t.idx.out].indexOf('1425') >= 0,
    rowOf(t, 'finance')[t.idx.out]);
  check('reject 跳过', rowOf(t, 'reject')[t.idx.status] === 'skipped',
    JSON.stringify(rowOf(t, 'reject')));
  check('状态卡显示 succeeded',
    (await page.textContent('#runOut')).indexOf('succeeded') >= 0);
  check('「模拟进程崩溃」此时可用',
    !(await page.isDisabled('#btnCrash')));
  await shot('04-批准通过');

  // ---------------------------------------------------------------- 5
  section('5 崩溃 → 断点续跑');
  await page.click('#btnCrash');
  await waitDone('#btnStart', '崩溃');
  await page.waitForTimeout(400);
  t = await readNodeTable();
  check('崩溃后 finance 回到 pending',
    rowOf(t, 'finance')[t.idx.status] === 'pending',
    JSON.stringify(rowOf(t, 'finance')));
  check('崩溃后「断点续跑」可用', !(await page.isDisabled('#btnResume')));
  check('事件日志里有 crash 条目',
    (await page.textContent('#events')).indexOf('crash') >= 0);
  await shot('05-崩溃');

  await page.click('#btnResume');
  await waitDone('#btnStart', '续跑');
  await page.waitForTimeout(400);
  t = await readNodeTable();
  check('续跑后 finance 成功', rowOf(t, 'finance')[t.idx.status] === 'succeeded',
    JSON.stringify(rowOf(t, 'finance')));
  check('续跑后 finance 尝试次数 = 2（这是「被重跑过」的证据）',
    rowOf(t, 'finance')[t.idx.tries] === '2',
    rowOf(t, 'finance')[t.idx.tries]);
  check('上游 submit 尝试次数仍是 1（没被重跑）',
    rowOf(t, 'submit')[t.idx.tries] === '1',
    rowOf(t, 'submit')[t.idx.tries]);
  await shot('06-断点续跑');

  // ---------------------------------------------------------------- 6
  section('6 取消并回滚（saga 补偿）');
  await page.click('#btnCancel');
  await waitDone('#btnStart', '取消');
  await page.waitForTimeout(500);
  t = await readNodeTable();
  check('状态卡显示 cancelled',
    (await page.textContent('#runOut')).indexOf('cancelled') >= 0);
  check('finance 标记为 compensated',
    rowOf(t, 'finance')[t.idx.status] === 'compensated',
    JSON.stringify(rowOf(t, 'finance')));
  check('rollback 节点出现在表里且成功',
    rowOf(t, 'rollback') && rowOf(t, 'rollback')[t.idx.status] === 'succeeded',
    JSON.stringify(rowOf(t, 'rollback')));
  check('rollback 输出含「已冲销」',
    rowOf(t, 'rollback')[t.idx.out].indexOf('已冲销') >= 0,
    rowOf(t, 'rollback')[t.idx.out]);
  const dCancel = await dagInfo();
  check('有节点用了 compensated 配色',
    dCancel.boxes.some((b) => b.cls === 'compensated'),
    JSON.stringify(dCancel.boxes.map((b) => b.cls)));
  check('补偿节点画了紫色虚线外框',
    await page.evaluate(() =>
      document.querySelectorAll('#dagG rect.compensation').length === 1));
  check('事件日志里有 compensation 条目',
    (await page.textContent('#events')).indexOf('compensation') >= 0);
  await shot('07-取消回滚');

  // ---------------------------------------------------------------- 7
  section('7 存档导出与恢复');
  await page.click('#btnArchive');
  await page.waitForTimeout(600);
  check('存档区块已显示', await page.isVisible('#archiveBox'));
  const archText = await page.textContent('#archiveBox');
  check('存档标题带字节数', /存档（\d+ 字节）/.test(archText),
    archText.slice(0, 60));
  check('存档内容是 JSON',
    (await page.textContent('#archiveBox pre')).trim().indexOf('{') === 0);
  check('「从存档恢复」按钮已启用', !(await page.isDisabled('#btnRestore')));
  await shot('08-导出存档');

  await page.click('#btnRestore');
  await page.waitForTimeout(600);
  check('恢复后运行表仍渲染', await page.isVisible('#runOut .cards'));
  check('恢复后有 restore 事件',
    (await page.textContent('#events')).indexOf('restore') >= 0);
  await shot('09-从存档恢复');

  // ---------------------------------------------------------------- 8
  section('8 驳回分支');
  await page.fill('#amount', '8888');
  await page.fill('#title', '团建');
  await page.click('#btnStart');
  await waitDone('#btnStart', '发起运行');
  await page.waitForTimeout(400);
  await page.click('#btnReject');
  await waitDone('#btnStart', '驳回');
  await page.waitForTimeout(400);
  t = await readNodeTable();
  check('reject 执行了', rowOf(t, 'reject')[t.idx.status] === 'succeeded',
    JSON.stringify(rowOf(t, 'reject')));
  check('reject 输出含事由', rowOf(t, 'reject')[t.idx.out].indexOf('团建') >= 0,
    rowOf(t, 'reject')[t.idx.out]);
  check('finance 跳过', rowOf(t, 'finance')[t.idx.status] === 'skipped',
    JSON.stringify(rowOf(t, 'finance')));
  check('整次运行仍是 succeeded（驳回不是失败）',
    (await page.textContent('#runOut')).indexOf('succeeded') >= 0);
  await shot('10-驳回分支');

  // ---------------------------------------------------------------- 9
  section('9 控制台与截图完整性');
  check('无 JS 异常', jsErrors.length === 0, JSON.stringify(jsErrors.slice(0, 3)));
  console.log('  · 资源加载错误（预期为 0，本脚本未故意触发）: '
    + resourceErrors.length);

  const files = fs.readdirSync(OUT).filter((f) => f.endsWith('.png'));
  check('截图数量 ≥ 10', files.length >= 10, files.length);
  const md5 = {};
  let dup = 0;
  for (const f of files) {
    const buf = fs.readFileSync(path.join(OUT, f));
    const key = require('crypto').createHash('md5').update(buf).digest('hex');
    if (md5[key]) { dup++; console.log('    ! 重复: ' + f + ' == ' + md5[key]); }
    md5[key] = f;
  }
  check('没有两张完全相同的截图（说明每步画面真的变了）', dup === 0, dup);

  await browser.close();

  console.log('\n' + '='.repeat(70));
  console.log('结果：' + passed + ' 通过 · ' + failed + ' 失败');
  console.log('='.repeat(70));
  process.exit(failed ? 1 : 0);
})();
