package pl.put.poznan.server.rest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import pl.put.poznan.server.logic.DBUtils;

import java.io.IOException;
import java.util.List;

@RestController
@RequestMapping("/api")
public class ImageUploadController {

    private static final Logger logger = LoggerFactory.getLogger(Controller.class);

    @PostMapping("/image-upload")
    public ResponseEntity<String> handleImageUpload(@RequestParam("trash-id") String trashId, @RequestParam("image") MultipartFile image) {
        try {
            // save the image to a desired location
            byte[] bytes = image.getBytes();
            DBUtils db = new DBUtils();
            db.uploadImage(trashId, image.getContentType(), bytes);
            logger.debug("Image saved successfully");
            return ResponseEntity.ok("Image uploaded successfully!");
        } catch (IOException e) {
            logger.debug("Image couldn't be saved");
            return ResponseEntity.badRequest().body("Failed to upload image: " + e.getMessage());
        }
    }

    @GetMapping("/image-download")
    public byte[] get(@RequestParam(value="trash-id", defaultValue="") String trashId,
                      @RequestParam(value="img-number", defaultValue="") String imgNumber) {
        logger.debug("Hello, give me images!");
        DBUtils db = new DBUtils();
        return db.getImages(trashId, imgNumber);
    }
}
