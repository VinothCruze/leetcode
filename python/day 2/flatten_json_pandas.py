import pandas as pd

data = { 
    "name": "John",
    "address": {
        "city": "Berlin",
        "country": "Germany"
    },
    "job": {
        "title": "Engineer",
        "details": {
            "experience": 5,
            "skills": ["Python", "SQL"]
        }
    }
}

print(pd.json_normalize(data, sep='_'))
