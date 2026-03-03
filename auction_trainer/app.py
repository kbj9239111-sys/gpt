from flask import Flask, request, jsonify, render_template
import json, os, uuid
from datetime import datetime

app = Flask(__name__)
DATA_FILE = "data/auctions.json"
CAR_LIST_FILE = "data/car_list.json"
CAR_LIST_FALLBACK = "car_list.json"

def read_data():
    if not os.path.exists(DATA_FILE):
        return []
    with open(DATA_FILE, "r", encoding="utf-8") as f:
        try: return json.load(f)
        except: return []

def write_data(data):
    os.makedirs("data", exist_ok=True)
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)



def read_car_list():
    # prefer data/car_list.json, fallback to ./car_list.json
    path = CAR_LIST_FILE if os.path.exists(CAR_LIST_FILE) else CAR_LIST_FALLBACK
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        try:
            return json.load(f)
        except:
            return {}

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/car_list", methods=["GET"])
def get_car_list():
    return jsonify(read_car_list())

@app.route("/api/auctions", methods=["GET"])
def get_auctions():
    return jsonify(read_data())

@app.route("/api/auctions", methods=["POST"])
def save_auction():
    data = read_data()
    rec = request.json
    rec["id"] = str(uuid.uuid4())[:8]
    rec["savedAt"] = datetime.now().strftime("%Y-%m-%d %H:%M")
    data.append(rec)
    write_data(data)
    return jsonify({"ok": True, "id": rec["id"]})

@app.route("/api/auctions/<aid>", methods=["PUT"])
def update_auction(aid):
    data = read_data()
    for i, r in enumerate(data):
        if r.get("id") == aid:
            data[i].update(request.json)
            write_data(data)
            return jsonify({"ok": True})
    return jsonify({"ok": False}), 404

@app.route("/api/auctions/<aid>", methods=["DELETE"])
def delete_auction(aid):
    data = [r for r in read_data() if r.get("id") != aid]
    write_data(data)
    return jsonify({"ok": True})

if __name__ == "__main__":
    import threading, webbrowser, time
    def open_browser():
        time.sleep(1.2)
        webbrowser.open("http://127.0.0.1:5001")
    threading.Thread(target=open_browser, daemon=True).start()
    app.run(debug=False, port=5001)
