import requests
import json
import logging
import os
from groq import Groq

class HybridAIClient:
    def __init__(self, model="aadhaar-pulse-expert", base_url="http://localhost:11434", groq_api_key=None):
        self.model = model
        self.base_url = base_url.rstrip('/')
        self.api_url = f"{self.base_url}/api/chat"
        self.groq_api_key = groq_api_key or os.environ.get("GROQ_API_KEY")
        self.groq_client = Groq(api_key=self.groq_api_key) if self.groq_api_key else None

    def chat(self, messages, stream=False, provider="ollama"):
        """
        Sends a list of messages to the selected AI provider.
        """
        if provider == "groq":
            return self._chat_groq(messages, stream)
        else:
            return self._chat_ollama(messages, stream)

    def _chat_ollama(self, messages, stream=False):
        payload = {
            "model": self.model,
            "messages": messages,
            "stream": stream
        }

        try:
            response = requests.post(self.api_url, json=payload, timeout=5)
            response.raise_for_status()
            
            if stream:
                return self._handle_ollama_stream(response)
            else:
                return response.json().get('message', {}).get('content', '')
        except Exception as e:
            logging.error(f"Ollama connection error: {e}")
            return f"Error: Could not connect to Ollama. Ensure it's running at {self.base_url}"

    def _chat_groq(self, messages, stream=False):
        if not self.groq_client:
            return "Error: Groq API Key not found. Please provide it in the settings."
        
        try:
            # Shift model to a valid Groq model
            groq_model = "llama-3.3-70b-versatile"
            
            completion = self.groq_client.chat.completions.create(
                model=groq_model,
                messages=messages,
                stream=stream,
            )
            
            if stream:
                return (chunk.choices[0].delta.content or "" for chunk in completion)
            else:
                return completion.choices[0].message.content
        except Exception as e:
            logging.error(f"Groq error: {e}")
            return f"Error: Groq API call failed. {e}"

    def _handle_ollama_stream(self, response):
        """Generator for Ollama streaming responses."""
        for line in response.iter_lines():
            if line:
                chunk = json.loads(line)
                if 'message' in chunk:
                    yield chunk['message'].get('content', '')
                if chunk.get('done'):
                    break

def test_client():
    client = HybridAIClient()
    response = client.chat([{"role": "user", "content": "Hello!"}], provider="ollama")
    print(f"Ollama Response: {response}")

if __name__ == "__main__":
    test_client()
