/**
 * Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
 */
package com.example.loadGenerator;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import org.codehaus.jackson.JsonGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class EventGenerator implements InputOperator
{

    public final transient DefaultOutputPort<String> out = new DefaultOutputPort<String>();

    private int adsIdx = 0;
    private int eventsIdx = 0;
    private StringBuilder sb = new StringBuilder();

    protected int batchSize = 20;
    protected int waitTime = 1;
    
    private String pageID = UUID.randomUUID().toString();
    private String userID = UUID.randomUUID().toString();
    private final String[] eventTypes = new String[]{"view", "click", "purchase"};

    private static final transient Logger logger = LoggerFactory.getLogger(JsonGenerator.class);

    public int getNumCampaigns()
    {
        return numCampaigns;
    }

    public void setNumCampaigns(int numCampaigns)
    {
        this.numCampaigns = numCampaigns;
    }

    public int getNumAdsPerCampaign()
    {
        return numAdsPerCampaign;
    }

    public void setNumAdsPerCampaign(int numAdsPerCampaign)
    {
        this.numAdsPerCampaign = numAdsPerCampaign;
    }

    private int numCampaigns = 100;
    private int numAdsPerCampaign = 10;

    private List<String> ads;
    private Map<String, List<String>> campaigns;

    public EventGenerator()
    {
    }

    public void init()
    {
        this.campaigns = generateCampaigns();
        this.ads = flattenCampaigns();
    }

    public Map<String, List<String>> getCampaigns()
    {
        return campaigns;
    }

    /**
     * Generate a single element
     */
    public String generateElement()
    {
        if (adsIdx == ads.size()) {
            adsIdx = 0;
        }

        if (eventsIdx == eventTypes.length) {
            eventsIdx = 0;
        }

        sb.setLength(0);
        sb.append("{\"user_id\":\"");
        sb.append(pageID);
        sb.append("\",\"page_id\":\"");
        sb.append(userID);
        sb.append("\",\"ad_id\":\"");
        sb.append(ads.get(adsIdx++));
        sb.append("\",\"ad_type\":\"");
        sb.append("banner78"); // value is immediately discarded. The original generator would put a string with 38/5 = 7.6 chars. We put 8.
        sb.append("\",\"event_type\":\"");
        sb.append(eventTypes[eventsIdx++]);
        sb.append("\",\"event_time\":\"");
        sb.append(System.currentTimeMillis());
        sb.append("\",\"ip_address\":\"1.2.3.4\"}");

        return sb.toString();

    }

    /**
     * Generate a random list of ads and campaigns
     */
    private Map<String, List<String>> generateCampaigns()
    {
        Map<String, List<String>> adsByCampaign = new LinkedHashMap<>();
        for (int i = 0; i < numCampaigns; i++) {
            String campaign = UUID.randomUUID().toString();
            ArrayList<String> ads = new ArrayList<>();
            adsByCampaign.put(campaign, ads);
            for (int j = 0; j < numAdsPerCampaign; j++) {
                ads.add(UUID.randomUUID().toString());
            }
        }

        return adsByCampaign;
    }

    /**
     * Flatten into just ads
     */
    private List<String> flattenCampaigns()
    {
        // Flatten campaigns into simple list of ads
        List<String> ads = new ArrayList<>();
        for (Map.Entry<String, List<String>> entry : campaigns.entrySet()) {
            for (String ad : entry.getValue()) {
                ads.add(ad);
            }
        }

        return ads;
    }

    @Override
    public void emitTuples()
    {
        for (int index = 0; index < batchSize; ++index) {
            out.emit(generateElement());
        }
        if(waitTime > 0) {
          try {
            Thread.sleep(waitTime);
          } catch (Exception e) {
            //do nothing
          }
        }
    }

    @Override
    public void beginWindow(long windowId)
    {

    }

    @Override
    public void endWindow()
    {
        logger.info("End Window");
    }

    @Override
    public void setup(Context.OperatorContext context)
    {

    }

    @Override
    public void teardown()
    {

    }

    public int getBatchSize()
    {
      return batchSize;
    }

    public void setBatchSize(int batchSize)
    {
      this.batchSize = batchSize;
    }

    public int getWaitTime()
    {
      return waitTime;
    }

    public void setWaitTime(int waitTime)
    {
      this.waitTime = waitTime;
    }

}

