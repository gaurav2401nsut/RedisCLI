from server import Client

def test_client():
    client = Client()
    
    # Test `SET` and `GET`
    client.set("name", "Alice")
    print(client.get("name"))  # Expected: Alice

if __name__ == "__main__":
    test_client()