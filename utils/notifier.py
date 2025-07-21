# import yagmail
# from twilio.rest import Client
# from config import EMAIL_CONFIG, TWILIO_CONFIG

# def send_email(subject, content):
#     yag = yagmail.SMTP(EMAIL_CONFIG["sender"], EMAIL_CONFIG["password"])
#     yag.send(EMAIL_CONFIG["receiver"], subject, content)

# def send_whatsapp_alert(message):
#     client = Client(TWILIO_CONFIG["account_sid"], TWILIO_CONFIG["auth_token"])
#     client.messages.create(
#         body=message,
#         from_=TWILIO_CONFIG["from_number"],
#         to=TWILIO_CONFIG["to_number"]
#     )