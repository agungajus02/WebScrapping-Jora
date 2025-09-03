from flask import Flask, jsonify
import pandas as pd

app = Flask(__name__)

CSV_FILE = "jora_sharded_occ.csv"

@app.route("/data", methods=["GET"])
def get_data():
    df = pd.read_csv(CSV_FILE)

    # Grouping berdasarkan kolom 'kategori'
    grouped = df.groupby("title").apply(lambda x: x.to_dict(orient="records"))

    # Ubah ke dict biasa biar bisa di-JSON-kan
    result = grouped.to_dict()

    return jsonify(result)

if __name__ == "__main__":
    app.run(debug=True)
