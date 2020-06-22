package com.example.esjd.utils;

import com.example.esjd.pojo.Content;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

@Component
public class HtmlParseUtil {
//    public static void main(String[] args) throws IOException {
//        new HtmlParseUtil().parseJD("码出高效").forEach(System.out::println);
//    }

    //
    public List<Content> parseJD(String keywords) throws IOException {

        List<Content> goodslist = new ArrayList<>();
        Content content = null;

        // 获取请求 "https://search.jd.com/Search?keyword=java"
        // 前提：需要联网
        // 指定编码为utf-8
        String url = "https://search.jd.com/Search?keyword="+keywords+"&enc=utf-8";

        // 解析网页。（Jsoup返回Document就是浏览器的Document对象)
        Document document = Jsoup.parse(new URL(url), 30000);
        // 所有在js中使用的方法，在这里都能用
        // 获取商品列表的id
        Element element = document.getElementById("J_goodsList");

        System.out.println(element.html());

        // 获取所有的li元素
        Elements elements = element.getElementsByTag("li");
        // 获取元素中的内容，这里的 el 就是诶一个 li 标签
        for (Element el : elements) {
            content = new Content();
            // 对于图片特别多的网站，所有的图片都是延迟加载的  source-data-lazy-img
            String img = el.getElementsByTag("img").eq(0).attr("src");
            String price = el.getElementsByClass("p-price").eq(0).text();
            String title = el.getElementsByClass("p-name").eq(0).text();

            content.setImg(img);
            content.setPrice(price);
            content.setTitle(title);

            goodslist.add(content);

            System.out.println("==========================");
            System.out.println(img);
            System.out.println(price);
            System.out.println(title);
        }

        return goodslist;
    }
}
