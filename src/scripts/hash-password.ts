import bcrypt from 'bcrypt';

async function main() {
  const plain = 'mWC9Y45ORt';
  const hash = await bcrypt.hash(plain, 10);
  console.log('Hash:', hash);
}

main();
