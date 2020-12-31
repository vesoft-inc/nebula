/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"

namespace nebula {

std::string toHexStr(folly::StringPiece str) {
    static const char* hex[] = {
        "00", "01", "02", "03", "04", "05", "06", "07",
        "08", "09", "0A", "0B", "0C", "0D", "0E", "0F",
        "10", "11", "12", "13", "14", "15", "16", "17",
        "18", "19", "1A", "1B", "1C", "1D", "1E", "1F",
        "20( )", "21(!)", "22(\")", "23(#)", "24($)", "25(%)", "26(&)", "27(')",
        "28(()", "29())", "2A(*)", "2B(+)", "2C(,)", "2D(-)", "2E(.)", "2F(/)",
        "30(0)", "31(1)", "32(2)", "33(3)", "34(4)", "35(5)", "36(6)", "37(7)",
        "38(8)", "39(9)", "3A(:)", "3B(;)", "3C(<)", "3D(=)", "3E(>)", "3F(?)",
        "40(@)", "41(A)", "42(B)", "43(C)", "44(D)", "45(E)", "46(F)", "47(G)",
        "48(H)", "49(I)", "4A(J)", "4B(K)", "4C(L)", "4D(M)", "4E(N)", "4F(O)",
        "50(P)", "51(Q)", "52(R)", "53(S)", "54(T)", "55(U)", "56(V)", "57(W)",
        "58(X)", "59(Y)", "5A(Z)", "5B([)", "5C(\\)", "5D(])", "5E(^)", "5F(_)",
        "60(`)", "61(a)", "62(b)", "63(c)", "64(d)", "65(e)", "66(f)", "67(g)",
        "68(h)", "69(i)", "6A(j)", "6B(k)", "6C(l)", "6D(m)", "6E(n)", "6F(o)",
        "70(p)", "71(q)", "72(r)", "73(s)", "74(t)", "75(u)", "76(v)", "77(w)",
        "78(x)", "79(y)", "7A(z)", "7B({)", "7C(|)", "7D(})", "7E(~)", "7F",
        "80", "81", "82", "83", "84", "85", "86", "87",
        "88", "89", "8A", "8B", "8C", "8D", "8E", "8F",
        "90", "91", "92", "93", "94", "95", "96", "97",
        "98", "99", "9A", "9B", "9C", "9D", "9E", "9F",
        "A0", "A1", "A2", "A3", "A4", "A5", "A6", "A7",
        "A8", "A9", "AA", "AB", "AC", "AD", "AE", "AF",
        "B0", "B1", "B2", "B3", "B4", "B5", "B6", "B7",
        "B8", "B9", "BA", "BB", "BC", "BD", "BE", "BF",
        "C0", "C1", "C2", "C3", "C4", "C5", "C6", "C7",
        "C8", "C9", "CA", "CB", "CC", "CD", "CE", "CF",
        "D0", "D1", "D2", "D3", "D4", "D5", "D6", "D7",
        "D8", "D9", "DA", "DB", "DC", "DD", "DE", "DF",
        "E0", "E1", "E2", "E3", "E4", "E5", "E6", "E7",
        "E8", "E9", "EA", "EB", "EC", "ED", "EE", "EF",
        "F0", "F1", "F2", "F3", "F4", "F5", "F6", "F7",
        "F8", "F9", "FA", "FB", "FC", "FD", "FE", "FF"};

    if (str.empty()) {
        return std::string();
    }

    std::string buf;
    buf.reserve(str.size() * 3 - 1);

    buf.append(hex[static_cast<uint8_t>(str[0])]);
    for (size_t i = 1; i < str.size(); i++) {
        buf.append(" ");
        buf.append(hex[static_cast<uint8_t>(str[i])]);
    }

    return buf;
}

}   // namespace nebula
