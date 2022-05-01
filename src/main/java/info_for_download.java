import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.util.ArrayList;
import java.util.List;


public class info_for_download {
    public String url;
    public int doc_type;
    public int depth;

    public info_for_download(String url, int doc_type, int depth){
        this.url = url;
        this.doc_type = doc_type;
        this.depth = depth;
    }
}
