/*
 * Title: CrawlerTest.java
 * Project: Jawlergram
 * Creator: Georgii Mikriukov
 * 2019
 */

import com.crawlergram.crawler.CrawlerSettings;
import com.crawlergram.crawler.TelegramCrawler;

public class CrawlerTest {

    public static void main(String[] args) {

        CrawlerSettings cs = new CrawlerSettings("res/api.cfg", "res/crawler.cfg", "res/storage.cfg");

        TelegramCrawler tc = new TelegramCrawler(cs);

        tc.saveOnlyMessagesToHDD();

        System.exit(0);

    }

}
