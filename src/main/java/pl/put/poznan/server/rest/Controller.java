package pl.put.poznan.server.rest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;
import pl.put.poznan.server.logic.DBUtils;


@RestController
@RequestMapping(value = "/{text}")
public class Controller {

    private static final Logger logger = LoggerFactory.getLogger(Controller.class);

    @RequestMapping(method = RequestMethod.GET, produces = "application/json")
    public String get(@PathVariable String text,
                              @RequestParam(value="function", defaultValue="") String[] function) {

        // log the parameters
        logger.debug(text);

        // perform requested function
        DBUtils db = new DBUtils();
        return db.functionSelector(function[0], text);
    }
}


