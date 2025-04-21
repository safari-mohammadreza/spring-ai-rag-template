package com.diaco.aranegar.util;

import com.diaco.aranegar.exception.JwtTokenExpiredException;
import io.jsonwebtoken.ExpiredJwtException;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.MalformedJwtException;
import io.jsonwebtoken.UnsupportedJwtException;
import io.jsonwebtoken.io.Decoders;
import io.jsonwebtoken.security.Keys;
import io.jsonwebtoken.security.SignatureException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
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

    public boolean validateJwtToken(String authToken) {
        try {
            Jwts.parserBuilder().setSigningKey(key()).build().parse(authToken);
            return true;
        } catch (MalformedJwtException e) {
            log.error("Invalid JWT token: {}", e.getMessage());
        } catch (ExpiredJwtException e) {
            log.error("JWT token is expired: {}", e.getMessage());
            throw new JwtTokenExpiredException("JWT token is expired", HttpStatus.UNAUTHORIZED);
        } catch (UnsupportedJwtException e) {
            log.error("JWT token is unsupported: {}", e.getMessage());
        } catch (IllegalArgumentException e) {
            log.error("JWT claims string is empty: {}", e.getMessage());
        }
        return false;
    }

    public boolean jwtIsExpired(String authToken) {
        try {
            Jwts.parserBuilder().setSigningKey(key()).build().parse(authToken);
            return false;
        } catch (MalformedJwtException e) {
            log.error("Invalid JWT token: {}", e.getMessage());
        } catch (ExpiredJwtException e) {
            log.error("JWT token is expired: {}", e.getMessage());
            return true;
        } catch (UnsupportedJwtException e) {
            log.error("JWT token is unsupported: {}", e.getMessage());
        } catch (IllegalArgumentException e) {
            log.error("JWT claims string is empty: {}", e.getMessage());
        } catch (SignatureException e) {
            log.error("JWT signature is invalid: {}", e.getMessage());
        }
        return false;
    }

    private Key key() {
        return Keys.hmacShaKeyFor(Decoders.BASE64.decode(jwtSecret));
    }

}
