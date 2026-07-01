function escapeHtml(text: string): string {
  return text.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
}

function applyBold(line: string): string {
  return line.replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
}

/**
 * A minimal, safe markdown renderer for agent replies: escapes all HTML first
 * (so nothing the model emits can inject markup), then supports only
 * "**bold**" and "- " bullet lists — the two things the system prompt asks
 * the agent to use.
 */
export function renderMarkdownLite(text: string): string {
  const lines = escapeHtml(text).split('\n')
  const html: string[] = []
  let inList = false

  for (const line of lines) {
    const bulletMatch = line.match(/^- (.*)/)
    if (bulletMatch) {
      if (!inList) {
        html.push('<ul>')
        inList = true
      }
      html.push(`<li>${applyBold(bulletMatch[1])}</li>`)
      continue
    }

    if (inList) {
      html.push('</ul>')
      inList = false
    }
    html.push(line.trim() === '' ? '<br/>' : `<p>${applyBold(line)}</p>`)
  }

  if (inList) html.push('</ul>')
  return html.join('')
}
