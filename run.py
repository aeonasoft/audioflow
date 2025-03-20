import uvicorn
import requests

public_ip = requests.get("https://api64.ipify.org").text
print(f"Audioflow is running at: http://{public_ip}:8000")

if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )