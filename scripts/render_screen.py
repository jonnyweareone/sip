#!/usr/bin/env python3
"""SONIQ Screen Image Renderer - generates branded PNG screens for Yealink phones."""
import sys, json, os, hashlib
from PIL import Image, ImageDraw, ImageFont

OUTDIR = "/opt/soniq-audio"

def get_fonts(h):
    try:
        return {
            "logo": ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", int(h*0.09)),
            "title": ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", int(h*0.055)),
            "body": ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", int(h*0.042)),
            "small": ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", int(h*0.035)),
        }
    except Exception:
        f = ImageFont.load_default()
        return {"logo": f, "title": f, "body": f, "small": f}

def render_base(w, h):
    img = Image.new("RGB", (w, h), (10, 10, 20))
    draw = ImageDraw.Draw(img)
    fonts = get_fonts(h)
    draw.text((w*0.05, h*0.04), "SONIQ", fill=(0, 160, 255), font=fonts["logo"])
    draw.line([(w*0.05, h*0.18), (w*0.95, h*0.18)], fill=(40, 60, 100), width=1)
    return img, draw, fonts

def render_call_summary(w, h, data):
    img, draw, fonts = render_base(w, h)
    draw.text((w*0.05, h*0.22), "AI Call Summary", fill=(220, 220, 240), font=fonts["title"])
    sentiment = data.get("sentiment", "")
    sent_color = (100, 255, 130) if "Positive" in sentiment else (255, 130, 100)
    lines = [
        (data.get("company", "") + " - " + data.get("contact", "") + " - " + data.get("duration", ""), (255, 255, 255)),
        ("Discussed: " + data.get("topic", ""), (180, 190, 210)),
        ("Action: " + data.get("action", ""), (255, 200, 100)),
        ("Sentiment: " + sentiment, sent_color),
        ("Next: " + data.get("next", ""), (180, 190, 210)),
    ]
    y = h * 0.34
    for text, color in lines:
        draw.text((w*0.05, y), text, fill=color, font=fonts["body"])
        y += h * 0.09
    return img

def render_scam_alert(w, h, data):
    img, draw, fonts = render_base(w, h)
    conf = int(data.get("confidence", 0))
    color = (255, 60, 60) if conf > 80 else (255, 180, 50)
    draw.text((w*0.05, h*0.22), "SCAM ALERT", fill=color, font=fonts["title"])
    draw.text((w*0.55, h*0.04), str(conf) + "%", fill=color, font=fonts["logo"])
    lines = [
        ("Caller: " + data.get("caller", "Unknown"), (255, 255, 255)),
        ("Confidence: " + str(conf) + "%", color),
        ("Reason: " + data.get("reason", ""), (180, 190, 210)),
        ("DO NOT SHARE PERSONAL INFO", (255, 100, 100)),
    ]
    y = h * 0.36
    for text, c in lines:
        draw.text((w*0.05, y), text, fill=c, font=fonts["body"])
        y += h * 0.10
    return img

def render_pre_call(w, h, data):
    img, draw, fonts = render_base(w, h)
    draw.text((w*0.05, h*0.22), "Incoming Call Intel", fill=(220, 220, 240), font=fonts["title"])
    sentiment = data.get("sentiment", "Unknown")
    sent_color = (100, 255, 130) if "Positive" in sentiment else (255, 180, 100)
    lines = [
        (data.get("caller", "") + " - " + data.get("company", ""), (255, 255, 255)),
        ("Deal Stage: " + data.get("stage", "Unknown"), (100, 200, 255)),
        ("Last Contact: " + data.get("last_call", "Unknown"), (180, 190, 210)),
        ("Sentiment: " + sentiment, sent_color),
    ]
    y = h * 0.36
    for text, color in lines:
        draw.text((w*0.05, y), text, fill=color, font=fonts["body"])
        y += h * 0.10
    return img

def render_wallboard(w, h, data):
    img, draw, fonts = render_base(w, h)
    draw.text((w*0.05, h*0.22), "SONIQ Wallboard", fill=(220, 220, 240), font=fonts["title"])
    lines = [
        ("Calls Today: " + data.get("calls_today", "0") + "  |  Queue: " + data.get("queue", "0"), (255, 255, 255)),
        ("Avg Wait: " + data.get("avg_wait", "0:00") + "  |  SLA: " + data.get("sla", "0%"), (180, 190, 210)),
        ("Top Rep: " + data.get("top_rep", "N/A"), (100, 255, 130)),
        ("Hot Leads: " + data.get("hot_leads", "0") + " pending", (255, 200, 100)),
    ]
    y = h * 0.36
    for text, color in lines:
        draw.text((w*0.05, y), text, fill=color, font=fonts["body"])
        y += h * 0.10
    return img

def render_missed_call(w, h, data):
    img, draw, fonts = render_base(w, h)
    draw.text((w*0.05, h*0.22), "Missed Call", fill=(255, 100, 100), font=fonts["title"])
    caller = data.get("caller", "Unknown")
    company = data.get("company", "")
    number = data.get("number", "")
    time_str = data.get("time", "Just now")
    lines = [
        (caller + (" - " + company if company else ""), (255, 255, 255)),
        (number, (180, 190, 210)),
        ("Time: " + time_str, (180, 190, 210)),
        ("Rang for " + data.get("duration", "20s") + " - no answer", (255, 180, 100)),
    ]
    y = h * 0.36
    for text, color in lines:
        draw.text((w*0.05, y), text, fill=color, font=fonts["body"])
        y += h * 0.10
    return img

def render_voicemail(w, h, data):
    img, draw, fonts = render_base(w, h)
    draw.text((w*0.05, h*0.22), "New Voicemail", fill=(100, 200, 255), font=fonts["title"])
    caller = data.get("caller", "Unknown")
    company = data.get("company", "")
    duration = data.get("duration", "0:00")
    transcript = data.get("transcript", "")
    lines = [
        (caller + (" - " + company if company else ""), (255, 255, 255)),
        ("Duration: " + duration, (180, 190, 210)),
    ]
    if transcript:
        words = transcript.split()
        line = ""
        for word in words:
            if len(line + " " + word) > 45:
                lines.append((line, (160, 170, 190)))
                line = word
            else:
                line = (line + " " + word).strip()
        if line:
            lines.append((line, (160, 170, 190)))
    y = h * 0.36
    for text, color in lines[:7]:
        draw.text((w*0.05, y), text, fill=color, font=fonts["body"])
        y += h * 0.075
    return img

def render_transfer(w, h, data):
    img, draw, fonts = render_base(w, h)
    draw.text((w*0.05, h*0.22), "Transfer Call", fill=(220, 220, 240), font=fonts["title"])
    caller = data.get("caller", "Unknown")
    draw.text((w*0.05, h*0.34), "From: " + caller, fill=(255, 255, 255), font=fonts["body"])
    exts = data.get("extensions", "").split("|")
    y = h * 0.46
    for i, ext_info in enumerate(exts[:5]):
        parts = ext_info.split(":")
        name = parts[0] if parts else ""
        status = parts[1] if len(parts) > 1 else "Unknown"
        color = (100, 255, 130) if "Available" in status else (255, 180, 100) if "On Call" in status else (180, 190, 210)
        draw.text((w*0.05, y), str(i+1) + ". " + name + " - " + status, fill=color, font=fonts["body"])
        y += h * 0.08
    return img

def render_call_log(w, h, data):
    img, draw, fonts = render_base(w, h)
    draw.text((w*0.05, h*0.22), "Recent Calls", fill=(220, 220, 240), font=fonts["title"])
    entries = data.get("entries", "").split("|")
    y = h * 0.34
    for entry in entries[:7]:
        parts = entry.split(":")
        if len(parts) >= 3:
            direction = parts[0]
            name = parts[1]
            time_str = parts[2]
            icon = "< " if direction == "in" else "> " if direction == "out" else "x "
            color = (100, 255, 130) if direction == "in" else (180, 190, 210) if direction == "out" else (255, 100, 100)
            draw.text((w*0.05, y), icon + name + "  " + time_str, fill=color, font=fonts["body"])
            y += h * 0.075
    return img

TEMPLATES = {
    "call_summary": render_call_summary,
    "scam_alert": render_scam_alert,
    "pre_call": render_pre_call,
    "wallboard": render_wallboard,
    "missed_call": render_missed_call,
    "voicemail": render_voicemail,
    "transfer": render_transfer,
    "call_log": render_call_log,
}

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: render_screen.py <template> [--key value ...]")
        print("Templates: " + ", ".join(TEMPLATES.keys()))
        sys.exit(1)
    template = sys.argv[1]
    if template not in TEMPLATES:
        print("Unknown template: " + template)
        sys.exit(1)
    data = {}
    i = 2
    while i < len(sys.argv):
        if sys.argv[i].startswith("--") and i+1 < len(sys.argv):
            data[sys.argv[i][2:]] = sys.argv[i+1]
            i += 2
        else:
            i += 1
    for suffix, w, h in [("t58", 800, 480), ("t54", 480, 272)]:
        img = TEMPLATES[template](w, h, data)
        key = hashlib.md5((template + json.dumps(data, sort_keys=True)).encode()).hexdigest()[:8]
        path = OUTDIR + "/screen-" + template + "-" + key + "-" + suffix + ".png"
        img.save(path)
        print(suffix + ":" + path)
