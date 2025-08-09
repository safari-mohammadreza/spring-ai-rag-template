package com.template.springai.controller;

import com.template.springai.service.RagService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api")
public class ApiController {
    private final RagService ragService;

    public ApiController(RagService ragService) {
        this.ragService = ragService;
    }

    @PostMapping("/docs")
    public ResponseEntity<?> addDocument(@RequestBody Map<String, String> body) {
        String text = body.get("text");
        if (text == null || text.isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "missing 'text' in body"));
        }
        ragService.addDocument(text);
        return ResponseEntity.ok(Map.of("status", "ok"));
    }

    @GetMapping("/ask")
    public ResponseEntity<?> ask(@RequestParam String q, @RequestParam(defaultValue = "3") int topK) {
        if (q == null || q.isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "missing 'q' parameter"));
        }
        String answer = ragService.askWithContext(q, topK);
        return ResponseEntity.ok(Map.of("answer", answer));
    }
}