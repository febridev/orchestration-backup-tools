import subprocess
import os
from dotenv import load_dotenv

load_dotenv()

def gcloud_login(email=None, service_account_key_path=None):
    base_path = os.path.dirname(os.path.abspath(__file__))
    service_account_key_path = f"{base_path}/{service_account_key_path}" 
    try:
        #if any email found
        if email:
            subprocess.run(["gcloud", "auth", "login", email], check=True)
            print(f"Login successfully with email : {email}")
        # if any services account found
        elif service_account_key_path:
            subprocess.run(["gcloud", "auth", "activate-service-account", "--key-file", service_account_key_path], check=True)
            print(f"Login successfully with services account : {service_account_key_path}")
        else:
            raise ValueError("Login with email or service account key not found.")
    except subprocess.CalledProcessError as e:
        print(f"An error accoured: {e}")
    except ValueError as ve:
        print(ve)

if __name__ == '__main__':
    email = os.environ.get("AUTH_EMAIL")
    service_account = os.environ.get("SERVICE_ACCOUNT")
    gcloud_login(email,service_account)