import os
import hvac
from dotenv import load_dotenv
from Crypto.Cipher import AES
from Crypto.Protocol.KDF import PBKDF2
from Crypto.Random import get_random_bytes
import base64



def get_vault():
    """
        this function is get value from hashicorp vault
        get value from ke master_key and data_key
    """
    #load env variable
    load_dotenv()
    
    # initiate client vault
    client = hvac.Client(
        url=os.environ.get("VAULT_URL"),
        token=os.environ.get("VAULT_TOKEN"),
        verify=False
    )

    # Get secret
    # secret = client.secrets.kv.v1.read_secret(path='dbops/db_backup', mount_point='kv')
    secret = client.secrets.kv.read_secret_version(path='dbops/db_backup', mount_point='kv')

    return secret['data']
    # print(secret['data'])

def encryption_dkey(master_key, data_key):
    """
        this function is encrypt the data_key,
        but this function only call for the first time if doesn't have
        data_key with encrypted for put to the hashicorp vault
        this function is using library pycryptodome
        https://github.com/Legrandin/pycryptodome
    """
    # Generate salt
    salt = get_random_bytes(16)

    # Derive key using PBKDF2
    key = PBKDF2(master_key, salt, dkLen=32, count=1000000)

    # Prepare cipher
    cipher = AES.new(key, AES.MODE_CBC)

    # Pad data_key to be a multiple of 16 bytes
    padding_length = 16 - len(data_key) % 16
    padded_data_key = data_key + padding_length * chr(padding_length)

    # Encrypt data_key
    ct_bytes = cipher.encrypt(padded_data_key.encode('utf-8'))

    # Encode the salt, IV, and ciphertext for output
    encrypted_data = base64.b64encode(salt + cipher.iv + ct_bytes).decode('utf-8')

    print(padding_length)
    print(padded_data_key)
    return encrypted_data 

def decryption_dkey(master_key,encrypted_data):
    """
        this function is decrypt the data_key 
        and put as key during encrypt backup database file 
        this function will be call everytime encrypt backup database file 
        this function is using library pycryptodome
        https://github.com/Legrandin/pycryptodome


    """
    # decode the base64-encoded data
    encrypted_data_bytes = base64.b64decode(encrypted_data)

    # extract salt, IV, and chipertext
    salt = encrypted_data_bytes[:16]
    iv = encrypted_data_bytes[16:32]
    ct = encrypted_data_bytes[32:]

    # derive the key using PBKDF2
    key = PBKDF2(master_key,salt,dkLen=32,count=1000000)

    # prepare cipher
    cipher = AES.new(key,AES.MODE_CBC, iv=iv)

    # decrypt the ciphertext
    decrypted_padded_data_key = cipher.decrypt(ct)

    # unpad the decrypted data
    padding_length = decrypted_padded_data_key[-1]
    decrypted_data_key = decrypted_padded_data_key[:-padding_length].decode('utf-8')

    print(decrypted_data_key)
    return decrypted_data_key


def main():
    # get list key
    list_key=get_vault()

    # get master key
    master_key=list_key['data']['master_key']

    # get data key
    data_key=list_key['data']['data_key']

    # decrypted_datakey
    # decryption_dkey(master_key,'kiaeuiOnJBRtdWACfyO8QBjDLFsR48Du23lsAf2mSkVbXMYRKkgOHjOnXRKP1uZv')
    decryption_dkey(master_key,data_key)




if __name__ == "__main__":
    main()
