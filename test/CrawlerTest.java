/*
 * Title: CrawlerTest.java
 * Project: Jawlergram
 * Creator: Georgii Mikriukov
 * 2019
 */

import com.crawlergram.crawler.CrawlerSettings;
import com.crawlergram.crawler.CrawlingTypes;

public class CrawlerTest {

    public static void main(String[] args) {

        CrawlerSettings cs = new CrawlerSettings("res/api.cfg", "res/crawler.cfg", "res/storage.cfg");

        CrawlingTypes.saveOnlyMessagesToHDD(cs);

        System.exit(0);

    }

}
