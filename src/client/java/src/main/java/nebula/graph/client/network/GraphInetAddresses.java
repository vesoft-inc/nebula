/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

package nebula.graph.client.network;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Locale;

public class GraphInetAddresses {

    private static int IPV4_PART_COUNT = 4;
    private static int IPV6_PART_COUNT = 8;

    public static boolean isInetAddress(String ipString) {
        return ipStringToBytes(ipString) != null;
    }

    private static byte[] ipStringToBytes(String ipString) {

        boolean hasColon = false;
        boolean hasDot = false;
        for (int i = 0; i < ipString.length(); i++) {
            char c = ipString.charAt(i);
            if (c == '.') {
                hasDot = true;
            } else if (c == ':') {
                if (hasDot) {
                    return null;
                }
                hasColon = true;
            } else if (Character.digit(c, 16) == -1) {
                return null;
            }
        }

        if (hasColon) {
            if (hasDot) {
                ipString = convertDottedQuadToHex(ipString);
                if (ipString == null) {
                    return null;
                }
            }
            return textToNumericFormatV6(ipString);
        } else if (hasDot) {
            return textToNumericFormatV4(ipString);
        }
        return null;
    }

    private static String convertDottedQuadToHex(String ipString) {
        int lastColon = ipString.lastIndexOf(':');
        String initialPart = ipString.substring(0, lastColon + 1);
        String dottedQuad = ipString.substring(lastColon + 1);
        byte[] quad = textToNumericFormatV4(dottedQuad);
        if (quad == null) {
            return null;
        }
        String penultimate = Integer.toHexString(((quad[0] & 0xff) << 8) | (quad[1] & 0xff));
        String ultimate = Integer.toHexString(((quad[2] & 0xff) << 8) | (quad[3] & 0xff));
        return initialPart + penultimate + ":" + ultimate;
    }

    private static byte[] textToNumericFormatV4(String ipString) {
        String[] address = ipString.split("\\.", IPV4_PART_COUNT + 1);
        if (address.length != IPV4_PART_COUNT) {
            return null;
        }

        byte[] bytes = new byte[IPV4_PART_COUNT];
        try {
            for (int i = 0; i < bytes.length; i++) {
                bytes[i] = parseOctet(address[i]);
            }
        } catch (NumberFormatException ex) {
            return null;
        }

        return bytes;
    }

    private static byte parseOctet(String ipPart) {
        int octet = Integer.parseInt(ipPart);
        if (octet > 255 || (ipPart.startsWith("0") && ipPart.length() > 1)) {
            throw new NumberFormatException();
        }
        return (byte) octet;
    }

    private static byte[] textToNumericFormatV6(String ipString) {
        String[] parts = ipString.split(":", IPV6_PART_COUNT + 2);
        if (parts.length < 3 || parts.length > IPV6_PART_COUNT + 1) {
            return null;
        }

        int skipIndex = -1;
        for (int i = 1; i < parts.length - 1; i++) {
            if (parts[i].length() == 0) {
                if (skipIndex >= 0) {
                    return null;  // Can't have more than one ::
                }
                skipIndex = i;
            }
        }

        int partsHi;
        int partsLo;
        if (skipIndex >= 0) {
            partsHi = skipIndex;
            partsLo = parts.length - skipIndex - 1;
            if (parts[0].length() == 0 && --partsHi != 0) {
                return null;
            }
            if (parts[parts.length - 1].length() == 0 && --partsLo != 0) {
                return null;
            }
        } else {
            partsHi = parts.length;
            partsLo = 0;
        }

        int partsSkipped = IPV6_PART_COUNT - (partsHi + partsLo);
        if (!(skipIndex >= 0 ? partsSkipped >= 1 : partsSkipped == 0)) {
            return null;
        }

        ByteBuffer rawBytes = ByteBuffer.allocate(2 * IPV6_PART_COUNT);
        try {
            for (int i = 0; i < partsHi; i++) {
                rawBytes.putShort(parseHextet(parts[i]));
            }
            for (int i = 0; i < partsSkipped; i++) {
                rawBytes.putShort((short) 0);
            }
            for (int i = partsLo; i > 0; i--) {
                rawBytes.putShort(parseHextet(parts[parts.length - i]));
            }
        } catch (NumberFormatException ex) {
            return null;
        }
        return rawBytes.array();
    }

    private static short parseHextet(String ipPart) {
        int hextet = Integer.parseInt(ipPart, 16);
        if (hextet > 0xffff) {
            throw new NumberFormatException();
        }
        return (short) hextet;
    }

    public static String toUriString(InetAddress ip) {
        if (ip instanceof Inet6Address) {
            return "[" + toAddrString(ip) + "]";
        }
        return toAddrString(ip);
    }

    public static String toAddrString(InetAddress ip) {
        if (ip == null) {
            throw new NullPointerException("ip");
        }
        if (ip instanceof Inet4Address) {
            byte[] bytes = ip.getAddress();
            return (bytes[0] & 0xff) + "."
                    + (bytes[1] & 0xff) + "."
                    + (bytes[2] & 0xff) + "."
                    + (bytes[3] & 0xff);
        }
        if (!(ip instanceof Inet6Address)) {
            throw new IllegalArgumentException("ip");
        }
        byte[] bytes = ip.getAddress();
        int[] hextets = new int[IPV6_PART_COUNT];
        for (int i = 0; i < hextets.length; i++) {
            hextets[i] =  (bytes[2 * i] & 255) << 8 | bytes[2 * i + 1] & 255;
        }
        compressLongestRunOfZeroes(hextets);
        return hextetsToIPv6String(hextets);
    }

    private static void compressLongestRunOfZeroes(int[] hextets) {
        int bestRunStart = -1;
        int bestRunLength = -1;
        int runStart = -1;
        for (int i = 0; i < hextets.length + 1; i++) {
            if (i < hextets.length && hextets[i] == 0) {
                if (runStart < 0) {
                    runStart = i;
                }
            } else if (runStart >= 0) {
                int runLength = i - runStart;
                if (runLength > bestRunLength) {
                    bestRunStart = runStart;
                    bestRunLength = runLength;
                }
                runStart = -1;
            }
        }
        if (bestRunLength >= 2) {
            Arrays.fill(hextets, bestRunStart, bestRunStart + bestRunLength, -1);
        }
    }

    private static String hextetsToIPv6String(int[] hextets) {
        StringBuilder buf = new StringBuilder(39);
        boolean lastWasNumber = false;
        for (int i = 0; i < hextets.length; i++) {
            boolean thisIsNumber = hextets[i] >= 0;
            if (thisIsNumber) {
                if (lastWasNumber) {
                    buf.append(':');
                }
                buf.append(Integer.toHexString(hextets[i]));
            } else {
                if (i == 0 || lastWasNumber) {
                    buf.append("::");
                }
            }
            lastWasNumber = thisIsNumber;
        }
        return buf.toString();
    }

    public static InetAddress forString(String ipString) {
        byte[] addr = ipStringToBytes(ipString);
        if (addr == null) {
            throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "'%s' is not an IP string literal.", ipString)
            );
        }

        return bytesToInetAddress(addr);
    }

    private static InetAddress bytesToInetAddress(byte[] addr) {
        try {
            return InetAddress.getByAddress(addr);
        } catch (UnknownHostException e) {
            throw new AssertionError(e);
        }
    }
}
