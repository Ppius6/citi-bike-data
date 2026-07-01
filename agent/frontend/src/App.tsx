import { useEffect, useRef, useState } from 'react'
import type { FormEvent } from 'react'
import { renderMarkdownLite } from './markdownLite'
import './App.css'

interface Message {
  role: 'user' | 'agent'
  text: string
  queries?: string[]
}

interface ChatResponse {
  answer: string
  queries: string[]
}

const INITIAL_MESSAGE: Message = {
  role: 'agent',
  text: "Ask me anything about Citi Bike trip data — ride durations, station activity, rider types, bike types.",
}

function App() {
  const [messages, setMessages] = useState<Message[]>([INITIAL_MESSAGE])
  const [input, setInput] = useState('')
  const [isLoading, setIsLoading] = useState(false)
  const [isDark, setIsDark] = useState(() => localStorage.getItem('theme') !== 'light')
  const listRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    if (isDark) {
      document.documentElement.removeAttribute('data-theme')
      localStorage.setItem('theme', 'dark')
    } else {
      document.documentElement.setAttribute('data-theme', 'light')
      localStorage.setItem('theme', 'light')
    }
  }, [isDark])

  const scrollToBottom = () => {
    requestAnimationFrame(() => {
      listRef.current?.scrollTo({ top: listRef.current.scrollHeight, behavior: 'smooth' })
    })
  }

  const sendMessage = async (event: FormEvent) => {
    event.preventDefault()
    const question = input.trim()
    if (!question || isLoading) return

    setMessages((prev) => [...prev, { role: 'user', text: question }])
    setInput('')
    setIsLoading(true)
    scrollToBottom()

    try {
      const res = await fetch('/api/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ message: question }),
      })
      if (!res.ok) throw new Error(`Request failed: ${res.status}`)
      const data: ChatResponse = await res.json()
      setMessages((prev) => [...prev, { role: 'agent', text: data.answer, queries: data.queries }])
    } catch {
      setMessages((prev) => [
        ...prev,
        { role: 'agent', text: "Sorry, something went wrong reaching the agent. Please try again." },
      ])
    } finally {
      setIsLoading(false)
      scrollToBottom()
    }
  }

  return (
    <div className="page">
      <header className="header">
        <div className="header-brand">
          <span className="header-title">CITI BIKE</span>
          <span className="header-subtitle">chat with your trip data</span>
        </div>
        <button
          type="button"
          className="theme-toggle"
          role="switch"
          aria-checked={isDark}
          aria-label="Toggle dark mode"
          onClick={() => setIsDark((prev) => !prev)}
        >
          <span className="theme-toggle-thumb" />
        </button>
      </header>

      <div className="message-list" ref={listRef}>
        {messages.map((message, index) => (
          <div key={index} className={`message-row ${message.role}`}>
            <div className={`bubble ${message.role}`}>
              {message.role === 'agent' ? (
                <div dangerouslySetInnerHTML={{ __html: renderMarkdownLite(message.text) }} />
              ) : (
                message.text
              )}
              {message.queries && message.queries.length > 0 && (
                <details className="queries">
                  <summary>SQL used</summary>
                  {message.queries.map((query, queryIndex) => (
                    <pre key={queryIndex}>{query}</pre>
                  ))}
                </details>
              )}
            </div>
          </div>
        ))}
        {isLoading && (
          <div className="message-row agent">
            <div className="bubble agent thinking">Thinking…</div>
          </div>
        )}
      </div>

      <form className="composer" onSubmit={sendMessage}>
        <input
          type="text"
          value={input}
          onChange={(event) => setInput(event.target.value)}
          placeholder="e.g. What was the average ride duration for casual riders last weekend?"
          disabled={isLoading}
        />
        <button type="submit" disabled={isLoading || !input.trim()}>
          Send
        </button>
      </form>
    </div>
  )
}

export default App
