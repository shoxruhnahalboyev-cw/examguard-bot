# ExamGuard AI — deploy qo'llanmasi

Papkada: `examguard_bot.py`, `requirements.txt`, `.env.example`, `examguard.service`.

## 0. Birinchi navbatda
- @BotFather → `/revoke` → eski tokenlarni bekor qiling, yangisini oling.
- Tokenni FAQAT `.env` yoki Render "Environment" bo'limiga yozing, kodga yozmang.
- Python 3.10 yoki yangiroq kerak.

## A) Render (bepul web service)
1. Kodni (token va `.env`siz!) GitHub'ning **private** repositoriysiga yuklang.
2. Render → New → Web Service → repo. Sozlamalar:
   - Build Command: `pip install -r requirements.txt`
   - Start Command: `python examguard_bot.py`
3. Environment: `BOT_TOKEN`, `CREATOR_ID`. (`PORT` ni Render o'zi beradi, tegmang.)
   Python versiyasi eski chiqsa: `PYTHON_VERSION=3.12.8`.
4. UptimeRobot → New monitor → HTTP(s) → `https://SIZNING-NOM.onrender.com/health` → har 5 daqiqa.
5. **Zaxira nusxani tekshirish (majburiy):**
   - Telegramda botga shaxsiy chatda `/start` yuboring.
   - `/backup` yuboring. Chatda `examguard_backup.db` fayli paydo bo'lib, tepada qadalgan (pinned) turishi kerak.
   - Render'da "Manual Deploy → Deploy latest commit" qiling. Loglarda `♻️ Baza ... tiklandi` chiqishi va guruh/vazifalar saqlanib qolgani kerak.
   - Shu chatda boshqa xabarlarni QADAMANG va zaxira faylni o'chirmang: bot tiklash uchun aynan qadalgan faylni qidiradi.

Render bepul tarifi uxlab qolishi yoki qayta ishga tushishi mumkin. Skeyuler shunda to'xtaydi, 
bot uyg'onganda esa muddati o'tgan vazifalarni yopadi.

## B) VPS (Ubuntu) — eng ishonchli
```bash
sudo apt update && sudo apt install -y python3-venv
sudo useradd -r -m -d /opt/examguard examguard
# fayllarni /opt/examguard ga nusxalang, so'ng:
sudo -u examguard python3 -m venv /opt/examguard/venv
sudo -u examguard /opt/examguard/venv/bin/pip install -r /opt/examguard/requirements.txt
sudo -u examguard cp /opt/examguard/.env.example /opt/examguard/.env   # va tokenni yozing
sudo cp /opt/examguard/examguard.service /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl enable --now examguard
journalctl -u examguard -f      # loglar
```
Yangilash: faqat `examguard_bot.py` ni almashtiring va `sudo systemctl restart examguard`.
Baza `/var/lib/examguard/` da turadi, deploy unga tegmaydi.

## Muhim eslatmalar
- Bot guruhda **admin** bo'lishi shart (ban qilish va a'zolarni ko'rish uchun).
- Guruhning har bir admini avtomatik "Ustoz" hisoblanadi.
- Deploy paytida bir-ikki daqiqa ichida o'zgartirish kiritmang (eski va yangi nusxa bir-birini qoplab ketmasligi uchun).
