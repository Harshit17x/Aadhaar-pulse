import requests
import json
import logging

class OllamaClient:
    def __init__(self, model="aadhaar-pulse-expert", base_url="http://localhost:11434"):
        self.model = model
        # Remove trailing slash if present
        self.base_url = base_url.rstrip('/')
        self.api_url = f"{self.base_url}/api/chat"

    def chat(self, messages, stream=False):
        """
        Sends a list of messages to the Ollama chat API.
        Args:
            messages: List of dicts with 'role' and 'content' keys.
            stream: Whether to stream the response.
        Returns:
            The full response string (if stream=False) or a generator.
        """
        payload = {
            "model": self.model,
            "messages": messages,
            "stream": stream
        }

        try:
            response = requests.post(self.api_url, json=payload, timeout=60)
            response.raise_for_status()
            
            if stream:
                return self._handle_stream(response)
            else:
                return response.json().get('message', {}).get('content', '')
        except Exception as e:
            logging.error(f"Ollama connection error: {e}")
            return f"Error: Could not connect to Ollama. Ensure it's running at {self.base_url}"

    def _handle_stream(self, response):
        """Generator for streaming responses."""
        for line in response.iter_lines():
            if line:
                chunk = json.loads(line)
                if 'message' in chunk:
                    yield chunk['message'].get('content', '')
                if chunk.get('done'):
                    break

def test_ollama():
    client = OllamaClient()
    response = client.chat([{"role": "user", "content": "Hello, how are you?"}])
    print(f"Ollama Response: {response}")

if __name__ == "__main__":
    test_ollama()
