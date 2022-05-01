import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.util.ArrayList;
import java.util.List;


public class info_for_parse {
    public Document doc;
    public int doc_type;
    public String link;
    public int depth;

    public info_for_parse(Document doc, int doc_type, String link, int depth){
        this.doc = doc;
        this.doc_type = doc_type;
        this.link = link;
        this.depth = depth;
    }
};
