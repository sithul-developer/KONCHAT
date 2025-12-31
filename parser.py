import re
from datetime import datetime

def parse_daily_report(text: str) -> dict:
    """
    Parses a daily sales report text and returns structured data.

    Returns:
        dict: {
            station_name: str,
            manager_name: str,
            report_date: str (ISO format),
            fuels: list of dicts {fuel_type, volume, amount, pos_variance},
            total_volume: float,
            total_amount: float,
            gain_loss: float,
            status: str
        }
    """
    data = {}

    # Helper to extract first matching group
    def find(pattern: str):
        match = re.search(pattern, text)
        return match.group(1).strip() if match else None

    # ---------------- BASIC INFO ----------------
    data["station_name"] = find(r"សាខាស្ថានីយ:\s*(.+)")
    data["manager_name"] = find(r"ឈ្មោះប្រធានស្ថានីយ\s*:\s*(.+)")

    raw_date = find(r"កាលបរិច្ឆេទ.*?:\s*(\d{2}/\d{2}/\d{4})")
    if raw_date:
        try:
            data["report_date"] = datetime.strptime(raw_date, "%m/%d/%Y").date().isoformat()
        except ValueError:
            data["report_date"] = None
    else:
        data["report_date"] = None

    # ---------------- FUEL DETAILS ----------------
    fuel_pattern = (
        r"\*\s*(.*?)\n"               # Fuel type
        r"- ចំនួនលក់សរុប:\s*([\d.]+)L\n"  # Volume
        r"- ចំនួនទឹកប្រាក់សរុបប្រចាំថ្ងៃ:\s*\$([\d.]+)\n"  # Amount
        r"- ផ្ទៀងផ្ទាត់:\s*(.*)"   # POS variance
    )

    fuels = []
    for fuel_type, volume, amount, pos_var in re.findall(fuel_pattern, text):
        try:
            fuels.append({
                "fuel_type": fuel_type.strip(),
                "volume": float(volume),
                "amount": float(amount),
                "pos_variance": pos_var.strip()
            })
        except ValueError:
            # Skip invalid numeric entries
            continue

    data["fuels"] = fuels

    # ---------------- TOTALS ----------------
    total_match = re.search(r"TOTAL SALES:\s*([\d.]+)L\s*\|\s*\$([\d.]+)", text)
    if total_match:
        try:
            data["total_volume"] = float(total_match.group(1))
            data["total_amount"] = float(total_match.group(2))
        except ValueError:
            data["total_volume"] = 0
            data["total_amount"] = 0
    else:
        data["total_volume"] = 0
        data["total_amount"] = 0

    # ---------------- GAIN / LOSS ----------------
    gl_match = re.search(r"Gain/lose:\s*([-\d.]+).*?(Gain|Lose)", text, re.IGNORECASE)
    if gl_match:
        try:
            data["gain_loss"] = float(gl_match.group(1))
            data["status"] = gl_match.group(2).capitalize()
        except ValueError:
            data["gain_loss"] = 0
            data["status"] = None
    else:
        data["gain_loss"] = 0
        data["status"] = None

    return data
