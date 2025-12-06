import crypto from 'crypto';

class CryptoService {
  private algorithm: string;
  private secretKey: Buffer;
  private iv: Buffer;

  constructor() {
    this.algorithm = 'aes-256-cbc';
    this.secretKey = Buffer.from(process.env.SECRET_KEY_ENCRYPT as string, 'hex');
    this.iv = Buffer.from(process.env.SECRET_IV_ENCRYPT as string, 'hex');
  }
  static async sha512(str: string): Promise<string> {
    return new Promise((resolve, reject) => {
      try {
        const hash = crypto.createHash('sha512');
        hash.update(str);
        const result = hash.digest('hex');
        resolve(result);
      } catch (error: any) {
        reject(new Error(`Failed to hash the string: ${error.message}`));
      }
    });
  }

  encrypt(text: string): string {
    const cipher = crypto.createCipheriv(this.algorithm, this.secretKey, this.iv);
    let encrypted = cipher.update(text, 'utf-8', 'hex');
    encrypted += cipher.final('hex');
    return `${this.iv.toString('hex')}:${encrypted}`;
  }

  decrypt(encryptedText: string): string {
    const [ivHex, encrypted] = encryptedText.split(':');
    const decipher = crypto.createDecipheriv(
      this.algorithm,
      this.secretKey,
      Buffer.from(ivHex, 'hex')
    );
    let decrypted = decipher.update(encrypted, 'hex', 'utf-8');
    decrypted += decipher.final('utf-8');
    return decrypted;
  }
}

export default CryptoService;
