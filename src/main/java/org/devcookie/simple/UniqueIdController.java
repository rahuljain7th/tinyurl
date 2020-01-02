package org.devcookie.simple;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class UniqueIdController {
    @RequestMapping("/giveMeId")
    String nextId() {
        return "Hello World!";
    }

}
