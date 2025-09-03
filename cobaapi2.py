from flask import Flask, jsonify
import requests
import csv
import io

app = Flask(__name__)

CSV_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vQYRZlngxF5zjtQ97vo8RmVQOrtkWAz4KPyRzqXizFlfypAanuPwoCztFi8b0T9_R43Vjmq__80UA0m/pub?output=csv"

@app.route("/data", methods=["GET"])
def get_data():
    # Ambil CSV dari Google Sheets
    response = requests.get(CSV_URL)
    response.encoding = 'utf-8'
    
    # Baca CSV
    csv_text = io.StringIO(response.text)
    reader = csv.DictReader(csv_text)
    
    # Convert ke list of dict (JSON)
    data = [row for row in reader]
    
    return jsonify(data)

if __name__ == "__main__":
    app.run(debug=True)
