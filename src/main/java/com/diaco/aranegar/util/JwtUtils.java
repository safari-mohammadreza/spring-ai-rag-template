package com.diaco.aranegar.util;

import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.io.Decoders;
import io.jsonwebtoken.security.Keys;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.security.Key;
import java.util.HashMap;
import java.util.Map;

@Component
@Slf4j
public class JwtUtils {

    private final String TOKEN_PREFIX = "Bearer ";

    @Value("${jwtSecret}")
    private String jwtSecret;


    public String getUserNameFromJwtToken(String token) {
        String tokenWithoutBearer = token.substring(TOKEN_PREFIX.length());
        return extractSubjects(tokenWithoutBearer).get("username");
    }

    public Map<String, String> extractSubjects(String token) {
        Map<String, String> subjects = new HashMap<>();
        Map<String, Object> claims = Jwts.parserBuilder()
                .setSigningKey(key())
                .build()
                .parseClaimsJws(token)
                .getBody();

        subjects.put("username", claims.get("username").toString());
        subjects.put("role", claims.get("role").toString());

        return subjects;
    }

    private Key key() {
        return Keys.hmacShaKeyFor(Decoders.BASE64.decode(jwtSecret));
    }

}
