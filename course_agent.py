from google import genai
from google.genai import types
import os
import json
from maps_golf_lookup import get_secret, project_id

# Environment reference for Gemini key
SECRET_GEMINI = os.environ.get("SECRET_GEMINI")

def get_gemini_client():
    api_key = get_secret(SECRET_GEMINI)
    if not api_key:
        raise ValueError("Gemini API key not found in Secret Manager")
    return genai.Client(api_key=api_key)

def enrich_course_details(course_name, address):
    """
    Uses Gemini with Google Search to find the official website and phone number
    for a given golf course.
    """
    client = get_gemini_client()
    model_id = "gemini-2.5-flash-lite"
    
    prompt = f"""
    Find the official website and primary phone number for the following golf course:
    Name: {course_name}
    Address: {address}
    
    Return the result in strictly JSON format with exactly these keys:
    "website": "string or null",
    "phone": "string or null"
    
    Ensure the website is the official one, not a third-party booking site if possible.
    """
    
    try:
        response = client.models.generate_content(
            model=model_id,
            contents=prompt,
            config=types.GenerateContentConfig(
                tools=[types.Tool(google_search=types.GoogleSearch())]
            )
        )
        
        # Parse JSON from response
        if response.text:
            text = response.text.strip()
            # Handle potential markdown code blocks
            if "```json" in text:
                text = text.split("```json")[-1].split("```")[0].strip()
            elif "```" in text:
                text = text.split("```")[-1].split("```")[0].strip()
            
            # Robust heuristic to find the first balanced JSON object
            start = text.find('{')
            if start != -1:
                brace_count = 0
                for i in range(start, len(text)):
                    if text[i] == '{':
                        brace_count += 1
                    elif text[i] == '}':
                        brace_count -= 1
                        if brace_count == 0:
                            text = text[start:i+1]
                            break
                
            data = json.loads(text)
            return data
    except Exception as e:
        print(f"Error in Gemini enrichment: {e}")
        
    return {"website": None, "phone": None}

if __name__ == "__main__":
    # Test
    test_course = "Langston Golf Course"
    test_addr = "2600 Benning Rd NE, Washington, DC 20002"
    result = enrich_course_details(test_course, test_addr)
    print(json.dumps(result, indent=2))
