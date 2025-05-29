import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

public class RDBParser {

    public static void parseInitialRDBFile(Config config) {

        if (config.rdbFile() != null && config.rdbFile().exists()) {
            try(BufferedInputStream bis = new BufferedInputStream(new FileInputStream(config.rdbFile()))) {
                RDBParser.parseRDB(bis);
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException("Cannot read keys");
            }
        }
    }

    public static void parseRDB(BufferedInputStream bis) throws IOException {

        DataInputStream dis = new DataInputStream(bis);

        String magicString = readMagicString(dis);
        if (!magicString.equals("REDIS")) {
            throw new IOException("Not a valid RDB file");
        }

        String versionNumber = readVersion(dis);
//        System.out.println("HEADER SECTION: " + magicString + versionNumber);

        while (dis.available() > 0) {
            int opCode = dis.readUnsignedByte();

            if (opCode == 0xFA) { // Metadata section
                String key = readString(dis);
                String value = readString(dis);
//                System.out.println("Metadata: " + key + " = " + value);
            } else if (opCode == 0xFE) { // Database selector
                int dbNumber = dis.readUnsignedByte();
//                System.out.println("\nSwitched to database: " + dbNumber);
            } else if (opCode == 0xFB) { // RDB_OPCODE_RESIZEDB
                int dbHashTableSize = readLength(dis);
                int expiryHashTableSize = readLength(dis);
//                System.out.printf("Resize DB - keys: %d, expires: %d%n", dbHashTableSize, expiryHashTableSize);
            } else if (opCode == 0xFD) { // Expiry time (seconds)
                int expiry = readLittleEndianInt(dis);
//                System.out.println("Key with expiry: " + expiry);
                int valueType = dis.readUnsignedByte();
                if (valueType == 0x00) { // string
                    saveKeyValueToStorage(dis, TimeUnit.SECONDS.toMillis(expiry));
                } else {
                    throw new IOException("Unsupported value type after expiry (0xFD): " + valueType);
                }
            } else if (opCode == 0xFC) { // Expiry time (milliseconds)
                long expiry = readLittleEndianLong(dis);
//                System.out.println("Key with expiry (ms): " + expiry);
                int valueType = dis.readUnsignedByte();
                if (valueType == 0x00) {
                    saveKeyValueToStorage(dis, expiry);
                } else {
                    throw new IOException("Unsupported value type after expiry (0xFC): " + valueType);
                }
            } else if (opCode == 0xFF) { // End of an RDB file
//                System.out.println("End of RDB file.");
                break;
            } else {
                // Read Key
                saveKeyValueToStorage(dis, 0L);
            }
        }

        System.out.println("RDB file parsed successfully");
    }

    private static void saveKeyValueToStorage(DataInputStream dis, long expiry) throws IOException {

        String key = readString(dis);
        String value = readString(dis);
//        System.out.println("Key: " + key + ", Value: " + value);

        if (expiry != 0L) {
            Storage.setWithExpiry(key, value, expiry - System.currentTimeMillis());
        } else {
            Storage.set(key, value);
        }
    }

    private static String readMagicString(DataInputStream dis) throws IOException {

        byte[] magic = new byte[5];
        dis.readFully(magic);
        return new String(magic, StandardCharsets.UTF_8);
    }

    private static String readVersion(DataInputStream dis) throws IOException {

        byte[] versionBytes = new byte[4];
        dis.readFully(versionBytes);
        return new String(versionBytes, StandardCharsets.UTF_8);
    }

    private static int readLittleEndianInt(DataInputStream dis) throws IOException {

        byte[] bytes = new byte[4];
        dis.readFully(bytes);
        return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getInt();
    }

    private static long readLittleEndianLong(DataInputStream dis) throws IOException {

        byte[] bytes = new byte[8];
        dis.readFully(bytes);
        return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getLong();
    }

    private static String readString(DataInputStream dis) throws IOException {

        int firstByte = dis.readUnsignedByte();

        if ((firstByte & 0xC0) == 0x00) {
            // 6-bit length string
            int length = firstByte & 0x3F;
            byte[] bytes = dis.readNBytes(length);
            return new String(bytes);
        } else if ((firstByte & 0xC0) == 0x40) {
            // 14-bit length string
            int secondByte = dis.readUnsignedByte();
            int length = ((firstByte & 0x3F) << 8) | secondByte;
            byte[] bytes = dis.readNBytes(length);
            return new String(bytes);
        } else if (firstByte == 0x80) {
            // 32-bit length string
            int length = dis.readInt();
            byte[] bytes = dis.readNBytes(length);
            return new String(bytes);
        } else if (firstByte == 0xC0) {
            // Encoded as 8-bit int (INT8)
            int value = dis.readByte();
            return Integer.toString(value);
        } else if (firstByte == 0xC1) {
            // Encoded as 16-bit int (INT16), little-endian
            short value = Short.reverseBytes(dis.readShort());
            return Short.toString(value);
        } else if (firstByte == 0xC2) {
            // Encoded as 32-bit int (INT32), little-endian
            int value = Integer.reverseBytes(dis.readInt());
            return Integer.toString(value);
        } else {
            throw new IOException("Unsupported string encoding, first byte: " + String.format("0x%02X", firstByte));
        }
    }

    private static int readLength(DataInputStream dis) throws IOException {

        int first = dis.readUnsignedByte();
        if ((first & 0xC0) == 0x00) {
            return first & 0x3F;
        } else if ((first & 0xC0) == 0x40) {
            int second = dis.readUnsignedByte();
            return ((first & 0x3F) << 8) | second;
        } else if ((first & 0xC0) == 0x80) {
            return dis.readInt();
        } else {
            throw new IOException("Unsupported length encoding: " + String.format("0x%02X", first));
        }
    }
}
