package com.datafibers.kafka.connect;

import org.json.JSONObject;
import yahoofinance.Stock;
import yahoofinance.YahooFinance;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Test
 */
public class YahooFinanceStockHelper {

    public static final Map<String, String> portfolio;

    static {
        portfolio = new HashMap<>();
        portfolio.put("Top 10 IT Service", "ACN,CTSH,EPAM,GIB,DOX,SAIC,FORR,INFY,WIT,INXN");
        portfolio.put("Top 10 Technology", "GOOGL,MSFT,AMZN,BABA,ORCL,IBM,HPE,SAP,FB,EBAY");
        portfolio.put("Top 10 US Banks", "ASB,BANC,BXS,BAC,BOH,BK,BBT,JPM,COF,C");
        portfolio.put("Top 10 US Telecom", "WIN,FTR,CTL,CNSL,BCE,T,VZ,CHT,SHEN,ALSK");
        portfolio.put("Top 10 Life Insurance", "KNSL,SLF,MET,MFC,PRU,ANAT,FFG,LNC,PUK");
    }

    public static String getStockJson(String symbol, Boolean refresh) {

        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        df.setTimeZone(TimeZone.getTimeZone("America/New_York"));

        try {
            Stock stock = YahooFinance.get(symbol);
            JSONObject stockJson = new JSONObject()
                    .put("refresh_time", df.format(new Date()))
                    .put("symbol", stock.getSymbol())
                    .put("company_name", stock.getName())
                    .put("exchange", stock.getStockExchange())
                    .put("ask_size", stock.getQuote(refresh).getAskSize().intValue())
                    .put("bid_size", stock.getQuote(refresh).getBidSize().intValue())
                    .put("open_price", Double.valueOf(stock.getQuote(refresh).getOpen().toString()))
                    .put("ask_price", Double.valueOf(stock.getQuote(refresh).getAsk().toString()))
                    .put("bid_price", Double.valueOf(stock.getQuote(refresh).getBid().toString()))
                    .put("price",  Double.valueOf(stock.getQuote(refresh).getPrice().toString()));
            return stockJson.toString();
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }
        return null;
    }

    public static String getStockJson2(Stock stock, Boolean refresh) {

        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        df.setTimeZone(TimeZone.getTimeZone("America/New_York"));

        try {
            //Stock stock = YahooFinance.get(symbol);
            JSONObject stockJson = new JSONObject()
                    .put("refresh_time", df.format(new Date()))
                    .put("symbol", stock.getSymbol())
                    .put("company_name", stock.getName())
                    .put("exchange", stock.getStockExchange())
                    .put("ask_size", stock.getQuote(refresh).getAskSize().intValue())
                    .put("bid_size", stock.getQuote(refresh).getBidSize().intValue())
                    .put("open_price", Double.valueOf(stock.getQuote(refresh).getOpen().toString()))
                    .put("ask_price", Double.valueOf(stock.getQuote(refresh).getAsk().toString()))
                    .put("bid_price", Double.valueOf(stock.getQuote(refresh).getBid().toString()))
                    .put("price",  Double.valueOf(stock.getQuote(refresh).getPrice().toString()));
            return stockJson.toString();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
        return null;
    }

    public static String getFakedStockJson(String symbol, String source) {
        JSONObject stockJson = null;
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        df.setTimeZone(TimeZone.getTimeZone("America/New_York"));
        Random rand = new Random();

        if (source.equalsIgnoreCase("PAST")) {
            try {
                Stock stock = YahooFinance.get(symbol);
                stockJson = new JSONObject().put("refresh_time", df.format(new Date()))
                        .put("symbol", stock.getSymbol())
                        .put("company_name", stock.getName() == null ?
                                symbol + " Inc.":stock.getName())
                        .put("exchange", stock.getStockExchange())
                        .put("ask_size", (stock.getQuote().getAskSize() == null ?
                                Integer.valueOf(30):stock.getQuote().getAskSize())
                                + Integer.valueOf(rand.nextInt(100)))
                        .put("bid_size", (stock.getQuote().getBidSize() == null ?
                                Integer.valueOf(45):stock.getQuote().getBidSize())
                                + Integer.valueOf(rand.nextInt(200)))
                        .put("open_price", (stock.getQuote().getOpen() == null ?
                                Double.valueOf(30):Double.valueOf(stock.getQuote().getOpen().toString()))
                                * rand.nextInt(3)/2)
                        .put("ask_price", (stock.getQuote().getAsk() == null ?
                                Double.valueOf(30):Double.valueOf(stock.getQuote().getAsk().toString()))
                                * rand.nextInt(3)/2)
                        .put("bid_price", (stock.getQuote().getBid() == null ?
                                Double.valueOf(50):Double.valueOf(stock.getQuote().getBid().toString()))
                                * rand.nextInt(3)/2)
                        .put("price",  (stock.getQuote().getPrice() == null ?
                                Double.valueOf(50):Double.valueOf(stock.getQuote().getPrice().toString()))
                                * rand.nextInt(3)/2)
                ;

            } catch (IOException ioe) {
                ioe.printStackTrace();
            }
        } else {
            stockJson = new JSONObject().put("refresh_time", df.format(new Date()))
                    .put("symbol", symbol)
                    .put("company_name", symbol)
                    .put("exchange", symbol)
                    .put("ask_size", Integer.valueOf(rand.nextInt(130)))
                    .put("bid_size", Integer.valueOf(rand.nextInt(230)))
                    .put("open_price", Double.valueOf(rand.nextInt(3) * 105/2))
                    .put("ask_price", Double.valueOf(rand.nextInt(3) * 102/2))
                    .put("bid_price", Double.valueOf(rand.nextInt(3) * 200/2))
                    .put("price",  Double.valueOf(rand.nextInt(3) * 120/2));
        }

        return stockJson.toString();
    }

    public static List<String> subdomainVisits(String[] cpdomains) {
        List<String> res = new ArrayList<>();
        Map<String, Integer> hs = new HashMap<>();
        for(String cpdomain : cpdomains) {
            int cnt = Integer.parseInt(cpdomain.split(" ")[0]);
            String domain = cpdomain.split(" ")[1];
            hs.put(domain, hs.getOrDefault(domain, 0) + cnt);
            while (domain.indexOf(".") > 0) {
                String subDomain = domain.substring(domain.indexOf(".") + 1);
                domain = subDomain;
                hs.put(domain, hs.getOrDefault(domain, 0) + cnt);
            }
        }
        // hs to list
        for(String domain : hs.keySet()) {
            res.add(domain + " " + hs.getOrDefault(domain, 0));
        }
        return res;
    }

    public static void main(String [] args) {
       String[] symbols = YahooFinanceStockHelper.portfolio.get("Top 10 Life Insurance").split(",");
        try {
            Map<String, Stock> stocks = YahooFinance.get(symbols);

            for (String symbol : symbols) {
                System.out.println(
                        YahooFinanceStockHelper.getStockJson2(stocks.get(symbol), false));
                //System.out.println(YahooFinanceStockHelper.getFakedStockJson(symbol, "PAST"));
            }
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }
}
