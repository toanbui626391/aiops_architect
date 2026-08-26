/**
 * Enterprise AIOps Platform - Architecture PDF Exporter
 * 
 * Converts all Markdown architectural specifications in docs/architecture/ into
 * professional, presentation-ready PDF documents with rendered Mermaid diagrams,
 * syntax-highlighted code blocks, GitHub-style alerts, and structured tables.
 */

const http = require('http');
const fs = require('fs');
const path = require('path');
const { execFile } = require('child_process');

const PROJECT_ROOT = path.resolve(__dirname, '..');
const DOCS_DIR = path.join(PROJECT_ROOT, 'docs', 'architecture');
const OUTPUT_DIR = path.join(PROJECT_ROOT, 'docs', 'pdf');

// Ensure output directory exists
if (!fs.existsSync(OUTPUT_DIR)) {
  fs.mkdirSync(OUTPUT_DIR, { recursive: true });
}

// Ordered list of architecture documents to export
const DOCUMENTS = [
  {
    id: '00',
    title: 'Enterprise AIOps Platform - Architecture Overview',
    domain: '00_overview',
    file: '00_overview/aiops_platform_overview.md',
    pdfName: '00_Enterprise_AIOps_Platform_Overview.pdf'
  },
  {
    id: '01',
    title: 'Ingestion Architecture & Streaming Pipelines',
    domain: '01_ingestion',
    file: '01_ingestion/ingestion_architecture.md',
    pdfName: '01_Ingestion_Architecture_and_Streaming_Pipelines.pdf'
  },
  {
    id: '02',
    title: 'SRE Observability Fleet - Source Telemetry Matrix',
    domain: '01_ingestion',
    file: '01_ingestion/source_telemetry_matrix.md',
    pdfName: '02_SRE_Observability_Source_Telemetry_Matrix.pdf'
  },
  {
    id: '03',
    title: 'Data Contracts, Canonical Schemas & Hybrid Cloud DLP',
    domain: '01_ingestion',
    file: '01_ingestion/data_contracts_and_schemas.md',
    pdfName: '03_Data_Contracts_and_Canonical_Schemas.pdf'
  },
  {
    id: '04',
    title: 'Ingestion Best Practices, SLIs & Watermarking',
    domain: '01_ingestion',
    file: '01_ingestion/ingestion_best_practices.md',
    pdfName: '04_Ingestion_Best_Practices_and_SLIs.pdf'
  },
  {
    id: '05',
    title: 'Unified Lakehouse, Data Processing & AI Feature Store Architecture',
    domain: '02_storage_and_lakehouse',
    file: '02_storage_and_lakehouse/lakehouse_and_feature_store.md',
    pdfName: '05_Unified_Lakehouse_and_AI_Feature_Store.pdf'
  },
  {
    id: '06',
    title: 'Autonomous Gemini SRE Agent & Supportive Reasoning Layer',
    domain: '03_intelligence_and_reasoning',
    file: '03_intelligence_and_reasoning/aiops_intelligence_layer.md',
    pdfName: '06_Autonomous_Gemini_SRE_Agent_and_Reasoning_Layer.pdf'
  },
  {
    id: '07',
    title: 'ServiceNow ITSM Integration, CMDB Sync & HITL Remediation',
    domain: '04_itsm_and_remediation',
    file: '04_itsm_and_remediation/servicenow_integration.md',
    pdfName: '07_ServiceNow_ITSM_Integration_and_Remediation.pdf'
  }
];

function getHtmlTemplate(docTitle, markdownContent, isCompleteMaster = false) {
  // Pre-process markdown alerts (> [!NOTE], > [!TIP], > [!IMPORTANT], > [!WARNING], > [!CAUTION])
  let processedMd = markdownContent
    .replace(/^>\s*\[!NOTE\]\s*([\s\S]*?)(?=\n\n|\n(?!>)|$)/gm, '<div class="alert alert-note"><div class="alert-title">ℹ️ NOTE</div><p>$1</p></div>')
    .replace(/^>\s*\[!TIP\]\s*([\s\S]*?)(?=\n\n|\n(?!>)|$)/gm, '<div class="alert alert-tip"><div class="alert-title">💡 TIP</div><p>$1</p></div>')
    .replace(/^>\s*\[!IMPORTANT\]\s*([\s\S]*?)(?=\n\n|\n(?!>)|$)/gm, '<div class="alert alert-important"><div class="alert-title">⚠️ IMPORTANT</div><p>$1</p></div>')
    .replace(/^>\s*\[!WARNING\]\s*([\s\S]*?)(?=\n\n|\n(?!>)|$)/gm, '<div class="alert alert-warning"><div class="alert-title">⚠️ WARNING</div><p>$1</p></div>')
    .replace(/^>\s*\[!CAUTION\]\s*([\s\S]*?)(?=\n\n|\n(?!>)|$)/gm, '<div class="alert alert-caution"><div class="alert-title">🛑 CAUTION</div><p>$1</p></div>');

  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>${docTitle}</title>
<!-- Mermaid & Markdown parser from CDN -->
<script src="https://cdn.jsdelivr.net/npm/marked/marked.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.min.js"></script>
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.9.0/styles/github-dark.min.css">
<script src="https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.9.0/highlight.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.9.0/languages/sql.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.9.0/languages/json.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.9.0/languages/yaml.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.9.0/languages/python.min.js"></script>

<style>
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&family=Fira+Code:wght@400;500;600&display=swap');

  @page {
    size: A4 portrait;
    margin: 16mm 14mm 16mm 14mm;
    @bottom-right {
      content: counter(page);
    }
  }

  * {
    box-sizing: border-box;
  }

  body {
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    font-size: 10.5pt;
    line-height: 1.6;
    color: #1f2937;
    background: #ffffff;
    margin: 0;
    padding: 0;
    -webkit-print-color-adjust: exact !important;
    print-color-adjust: exact !important;
  }

  /* Document Header Banner */
  .doc-header-banner {
    background: linear-gradient(135deg, #1e1b4b 0%, #312e81 50%, #4338ca 100%);
    color: #ffffff;
    padding: 24px 28px;
    border-radius: 10px;
    margin-bottom: 28px;
    box-shadow: 0 4px 12px rgba(30, 27, 75, 0.15);
  }
  .doc-header-banner .badge {
    display: inline-block;
    background: rgba(255, 255, 255, 0.2);
    color: #e0e7ff;
    font-size: 8pt;
    font-weight: 700;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    padding: 4px 10px;
    border-radius: 9999px;
    margin-bottom: 10px;
  }
  .doc-header-banner h1 {
    color: #ffffff;
    font-size: 20pt;
    font-weight: 800;
    margin: 0 0 8px 0;
    line-height: 1.25;
    border: none;
    padding: 0;
  }
  .doc-header-banner .meta-row {
    display: flex;
    gap: 20px;
    font-size: 8.5pt;
    color: #c7d2fe;
    margin-top: 12px;
    border-top: 1px solid rgba(255, 255, 255, 0.15);
    padding-top: 10px;
  }

  /* Headings */
  h1 {
    font-size: 16pt;
    font-weight: 700;
    color: #111827;
    margin-top: 28px;
    margin-bottom: 12px;
    padding-bottom: 6px;
    border-bottom: 2px solid #e5e7eb;
    page-break-after: avoid;
  }
  h2 {
    font-size: 13pt;
    font-weight: 700;
    color: #1f2937;
    margin-top: 22px;
    margin-bottom: 10px;
    page-break-after: avoid;
  }
  h3 {
    font-size: 11pt;
    font-weight: 600;
    color: #374151;
    margin-top: 16px;
    margin-bottom: 8px;
    page-break-after: avoid;
  }
  h4 {
    font-size: 10pt;
    font-weight: 600;
    color: #4b5563;
    margin-top: 12px;
    margin-bottom: 6px;
    page-break-after: avoid;
  }

  p, ul, ol {
    margin-top: 0;
    margin-bottom: 10px;
  }

  li {
    margin-bottom: 4px;
  }

  hr {
    border: 0;
    height: 1px;
    background: #e5e7eb;
    margin: 24px 0;
  }

  /* Tables */
  table {
    width: 100%;
    border-collapse: collapse;
    margin: 16px 0;
    font-size: 9pt;
    page-break-inside: avoid;
  }
  th, td {
    padding: 8px 10px;
    border: 1px solid #d1d5db;
    text-align: left;
    vertical-align: top;
  }
  th {
    background-color: #f3f4f6;
    font-weight: 700;
    color: #111827;
  }
  tr:nth-child(even) td {
    background-color: #fafafa;
  }

  /* Code & Syntax */
  code {
    font-family: 'Fira Code', Consolas, Monaco, monospace;
    font-size: 8.5pt;
    background-color: #f3f4f6;
    color: #b91c1c;
    padding: 2px 5px;
    border-radius: 4px;
  }
  pre {
    background-color: #0f172a;
    border-radius: 8px;
    padding: 12px 14px;
    overflow-x: auto;
    margin: 14px 0;
    page-break-inside: avoid;
  }
  pre code {
    background-color: transparent;
    color: #e2e8f0;
    padding: 0;
    font-size: 8.5pt;
    line-height: 1.45;
  }

  /* Mermaid Diagram Container */
  .mermaid-wrapper {
    margin: 20px 0;
    padding: 14px;
    background: #ffffff;
    border: 1px solid #e5e7eb;
    border-radius: 8px;
    display: flex;
    justify-content: center;
    page-break-inside: avoid;
    box-shadow: 0 1px 3px rgba(0,0,0,0.05);
  }
  .mermaid {
    width: 100%;
    display: flex;
    justify-content: center;
  }
  .mermaid svg {
    max-width: 100% !important;
    height: auto !important;
  }

  /* Alert Callouts */
  .alert {
    padding: 10px 14px;
    border-radius: 6px;
    margin: 14px 0;
    font-size: 9pt;
    page-break-inside: avoid;
  }
  .alert-title {
    font-weight: 700;
    font-size: 8.5pt;
    letter-spacing: 0.05em;
    margin-bottom: 4px;
    text-transform: uppercase;
  }
  .alert p {
    margin: 0;
  }
  .alert-note {
    background: #eff6ff;
    border-left: 4px solid #3b82f6;
    color: #1e40af;
  }
  .alert-tip {
    background: #f0fdf4;
    border-left: 4px solid #22c55e;
    color: #166534;
  }
  .alert-important {
    background: #faf5ff;
    border-left: 4px solid #a855f7;
    color: #6b21a8;
  }
  .alert-warning {
    background: #fffbeb;
    border-left: 4px solid #f59e0b;
    color: #92400e;
  }
  .alert-caution {
    background: #fef2f2;
    border-left: 4px solid #ef4444;
    color: #991b1b;
  }

  /* Blockquotes */
  blockquote {
    margin: 14px 0;
    padding: 8px 14px;
    background: #f9fafb;
    border-left: 4px solid #9ca3af;
    color: #4b5563;
    font-style: italic;
    page-break-inside: avoid;
  }

  /* Page Break Helpers */
  .page-break {
    page-break-before: always;
  }

  /* Footer */
  .doc-footer {
    margin-top: 30px;
    padding-top: 12px;
    border-top: 1px solid #e5e7eb;
    display: flex;
    justify-content: space-between;
    font-size: 7.5pt;
    color: #9ca3af;
  }
</style>
</head>
<body>

  <div class="doc-header-banner">
    <div class="badge">Enterprise AIOps Platform • Architecture Specification</div>
    <h1>${docTitle}</h1>
    <div class="meta-row">
      <div><b>Target Platform:</b> Google Cloud Platform (GCP)</div>
      <div><b>System of Record:</b> ServiceNow ITSM</div>
      <div><b>Version:</b> 2.0 (Production Blueprint)</div>
      <div><b>Generated:</b> August 2026</div>
    </div>
  </div>

  <div id="content"></div>

  <div class="doc-footer">
    <div>Enterprise AIOps Platform Architecture Specification</div>
    <div>Confidential & Proprietary • SRE Core Architecture</div>
  </div>

  <script type="text/markdown" id="raw-md">
${processedMd.replace(/<\/script>/gi, '<\\/script>')}
  </script>

  <script>
    mermaid.initialize({
      startOnLoad: false,
      theme: 'default',
      securityLevel: 'loose',
      fontFamily: 'Inter, sans-serif',
      fontSize: 12,
      flowchart: {
        useMaxWidth: true,
        htmlLabels: true,
        curve: 'basis'
      }
    });

    window.addEventListener('DOMContentLoaded', async () => {
      const raw = document.getElementById('raw-md').textContent;
      marked.setOptions({
        gfm: true,
        breaks: true,
        highlight: function(code, lang) {
          const language = hljs.getLanguage(lang) ? lang : 'plaintext';
          return hljs.highlight(code, { language }).value;
        }
      });

      const parsedHtml = marked.parse(raw);
      const contentEl = document.getElementById('content');
      contentEl.innerHTML = parsedHtml;

      // Transform mermaid codeblocks
      const mermaidPres = contentEl.querySelectorAll('pre code.language-mermaid');
      for (const pre of mermaidPres) {
        const parentPre = pre.parentElement;
        const codeText = pre.textContent;
        const wrapper = document.createElement('div');
        wrapper.className = 'mermaid-wrapper';
        const mDiv = document.createElement('div');
        mDiv.className = 'mermaid';
        mDiv.textContent = codeText;
        wrapper.appendChild(mDiv);
        parentPre.replaceWith(wrapper);
      }

      try {
        await mermaid.run();
      } catch (err) {
        console.error('Mermaid render error:', err);
      }

      // Add rendered flag
      const doneFlag = document.createElement('div');
      doneFlag.id = 'render-complete';
      document.body.appendChild(doneFlag);
    });
  </script>
</body>
</html>`;
}

// Convert markdown to PDF via headless Chrome
async function convertDocToPdf(serverPort, doc, chromePath) {
  const url = `http://localhost:${serverPort}/doc/${doc.id}`;
  const outPdf = path.join(OUTPUT_DIR, doc.pdfName);

  return new Promise((resolve, reject) => {
    const args = [
      '--headless=new',
      '--disable-gpu',
      '--no-pdf-header-footer',
      '--virtual-time-budget=6000',
      `--print-to-pdf=${outPdf}`,
      url
    ];

    execFile(chromePath, args, (err) => {
      if (err) {
        return reject(err);
      }
      if (fs.existsSync(outPdf)) {
        const size = (fs.statSync(outPdf).size / 1024).toFixed(1);
        console.log(`  ✅ [${doc.id}] ${doc.pdfName} (${size} KB)`);
        resolve(outPdf);
      } else {
        reject(new Error(`PDF file not created: ${outPdf}`));
      }
    });
  });
}

// Master combined document generator
function getMasterCombinedMarkdown() {
  let combinedMd = `# Enterprise AIOps Platform - Master Architecture Blueprint\n\n`;
  combinedMd += `> [!IMPORTANT]\n> This comprehensive document consolidates all 8 architectural domain specifications for the Enterprise AIOps Platform, covering Ingestion, Canonical Data Contracts, Analytical Lakehouse, AI Feature Store, Autonomous Gemini SRE Agent, and ServiceNow ITSM Integration.\n\n---\n\n`;

  for (const doc of DOCUMENTS) {
    const filePath = path.join(DOCS_DIR, doc.file);
    if (fs.existsSync(filePath)) {
      const content = fs.readFileSync(filePath, 'utf8');
      combinedMd += `\n<div class="page-break"></div>\n\n`;
      combinedMd += content + `\n\n---\n\n`;
    }
  }
  return combinedMd;
}

async function main() {
  console.log('================================================================');
  console.log('  Enterprise AIOps Platform - Architecture PDF Exporter');
  console.log('================================================================\n');

  const chromePath = 'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe';
  if (!fs.existsSync(chromePath)) {
    console.error(`❌ Chrome not found at ${chromePath}`);
    process.exit(1);
  }

  // Pre-load all documents
  const docContents = {};
  for (const doc of DOCUMENTS) {
    const filePath = path.join(DOCS_DIR, doc.file);
    if (fs.existsSync(filePath)) {
      docContents[doc.id] = fs.readFileSync(filePath, 'utf8');
    } else {
      console.warn(`⚠️ Warning: File not found: ${filePath}`);
    }
  }
  const masterContent = getMasterCombinedMarkdown();

  // Start local HTTP server
  const PORT = 3457;
  const server = http.createServer((req, res) => {
    const url = req.url;
    if (url.startsWith('/doc/')) {
      const docId = url.replace('/doc/', '');
      const doc = DOCUMENTS.find(d => d.id === docId);
      if (doc && docContents[doc.id]) {
        res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
        return res.end(getHtmlTemplate(doc.title, docContents[doc.id]));
      }
    } else if (url === '/master') {
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      return res.end(getHtmlTemplate('Enterprise AIOps Platform - Complete Master Architecture', masterContent, true));
    }

    res.writeHead(404, { 'Content-Type': 'text/plain' });
    res.end('Not Found');
  });

  server.listen(PORT, async () => {
    console.log(`🚀 Temporary HTTP Renderer active on http://localhost:${PORT}`);
    console.log(`📁 Target Output Directory: ${OUTPUT_DIR}\n`);

    try {
      // 1. Export individual PDFs
      console.log('📄 Exporting Individual Architecture Domain PDFs:');
      for (const doc of DOCUMENTS) {
        if (docContents[doc.id]) {
          await convertDocToPdf(PORT, doc, chromePath);
        }
      }

      // 2. Export complete consolidated master PDF
      console.log('\n📚 Exporting Consolidated Master Architecture PDF:');
      const masterDoc = {
        id: 'master',
        pdfName: 'Enterprise_AIOps_Platform_Complete_Architecture_Master.pdf'
      };
      const masterUrl = `http://localhost:${PORT}/master`;
      const masterPdfPath = path.join(OUTPUT_DIR, masterDoc.pdfName);

      await new Promise((resolve, reject) => {
        const args = [
          '--headless=new',
          '--disable-gpu',
          '--no-pdf-header-footer',
          '--virtual-time-budget=12000',
          `--print-to-pdf=${masterPdfPath}`,
          masterUrl
        ];

        execFile(chromePath, args, (err) => {
          if (err) return reject(err);
          if (fs.existsSync(masterPdfPath)) {
            const size = (fs.statSync(masterPdfPath).size / 1024).toFixed(1);
            console.log(`  🏆 [MASTER] ${masterDoc.pdfName} (${size} KB)`);
            resolve(masterPdfPath);
          } else {
            reject(new Error(`Master PDF file not created`));
          }
        });
      });

      console.log('\n================================================================');
      console.log(`🎉 All PDFs successfully generated in: docs/pdf/`);
      console.log('================================================================\n');

    } catch (err) {
      console.error('❌ PDF Generation failed:', err);
    } finally {
      server.close();
    }
  });
}

main();
