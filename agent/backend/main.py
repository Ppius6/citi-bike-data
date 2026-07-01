import os

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", "..", ".env"))

from agent import run_bike_agent

if __name__ == "__main__":
    print("🚴 Bike-Share Analytics Agent Active! Type 'exit' to quit.")
    print("-" * 50)

    while True:
        user_input = input("\nAsk a question about the bike data: ")
        if user_input.strip().lower() in ("exit", "quit", "stop"):
            break

        if user_input.strip():
            print("Thinking...")
            result = run_bike_agent(user_input)
            print(f"\n[Answer]:\n{result['answer']}")
