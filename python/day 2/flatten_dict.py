 
def flatten_dict(d, parent_key='', sep='_'):
    items = {}

    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k

        if isinstance(v, dict):
            # Recursively flatten dictionary
            items.update(flatten_dict(v, new_key, sep=sep))
        else:
            # Add non-dict values directly
            items[new_key] = v

    return items


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
for k, v in data.items():
    print(f"{k}: {v}")
output = flatten_dict(data)
print(output)


## to make it as json dict againa
flat_dict = df.to_dict(orient="records")[0]
print(flat_dict)
