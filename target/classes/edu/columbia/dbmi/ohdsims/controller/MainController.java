package edu.columbia.dbmi.ohdsims.controller;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Resource;
import javax.servlet.ServletContext;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import javax.sql.DataSource;

import com.alibaba.fastjson.JSON;
import edu.columbia.dbmi.ohdsims.pojo.*;
import edu.columbia.dbmi.ohdsims.tool.JSON2SQL;
import edu.columbia.dbmi.ohdsims.util.FileUtil;
import edu.columbia.dbmi.ohdsims.util.HttpUtil;
import net.sf.json.JSONArray;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.groovy.transform.SourceURIASTTransformation;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.support.ResourcePropertySource;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import edu.columbia.dbmi.ohdsims.service.IConceptFilteringService;
import edu.columbia.dbmi.ohdsims.service.IConceptMappingService;
import edu.columbia.dbmi.ohdsims.service.IInformationExtractionService;
import edu.columbia.dbmi.ohdsims.service.IQueryFormulateService;
import edu.stanford.nlp.util.Triple;
import net.sf.json.JSONObject;

import static edu.columbia.dbmi.ohdsims.pojo.GlobalSetting.ohdsi_api_base_url;

import org.apache.commons.text.similarity.LevenshteinDistance;

@Controller
@RequestMapping("/main")
public class MainController {
    private Logger logger = LogManager.getLogger(MainController.class);
    @Resource
    private IInformationExtractionService ieService;

    @Resource
    private IConceptMappingService conceptMappingService;

    @Resource
    private IQueryFormulateService qfService;

    @Resource
    private IConceptFilteringService cfService;

    @RequestMapping("/shownewpage")
    public String shownewPage() throws Exception {
        return "newPage";
    }

    @RequestMapping("/gojson")
    public String showJsonPage(HttpSession httpSession, HttpServletRequest request, ModelMap model) throws Exception {
        return "jsonPage";
    }

    @RequestMapping("/sqlpage")
    public String toSQLPage(HttpSession httpSession) throws Exception {
        return "sqlPage";
    }

    @RequestMapping("/slides")
    public String showsearchPICO(HttpSession httpSession) throws Exception {
        return "slidesPage";
    }


    @RequestMapping("/autoparse")
    @ResponseBody
    public Map<String, Object> runPipeLine(HttpSession httpSession, HttpServletRequest request, String nctid, String initialevent, String inc,
                                           String exc, boolean abb, String obstart, String obend, String daysbefore, String daysafter, String limitto) {
        Document doc = this.ieService.translateByDoc(initialevent, inc, exc);//Parse the document.
        doc = this.ieService.patchIEResults(doc);//Add "Demographic" term together with the "has_value" relation.
        if (abb == true) {
            doc = this.ieService.abbrExtensionByDoc(doc);//Extend the abbreviation.
        }
        List<ConceptSet> allsts = this.conceptMappingService.getAllConceptSets();//Get pre-existing Concept sets in a ConceptSet list format from http://api.ohdsi.org/WebAPI/conceptset/
        List<Term> terms = this.conceptMappingService.getDistinctTerm(doc);////Get the term list which only contains distinct terms with category: "Condition","Observation","Measurement","Drug","Procedure"
        Map<String, Integer> conceptSetIds = this.conceptMappingService.createConceptsByTerms(allsts, terms);//Get a map whose keys are the names of entities, and the values are the matched concept set IDs.
        doc = this.conceptMappingService.linkConceptSetsToTerms(doc, conceptSetIds);//Update the vocabularyId attribute of each term with the corresponding concept set Id.
        ObservationConstraint oc = new ObservationConstraint();
        oc.setDaysAfter(Integer.valueOf(daysafter));
        oc.setDaysBefore(Integer.valueOf(daysbefore));
        oc.setLimitTo(limitto);


        if (obstart.length() > 0) {
            oc.setStartDate(obstart);
        } else {
            oc.setStartDate(null);
        }
        if (obend.length() > 0) {
            oc.setEndDate(obend);
        } else {
            oc.setEndDate(null);
        }
        doc.setInitial_event_constraint(oc);
        httpSession.setAttribute("allcriteria", doc);
        Map<String, Object> map = new HashMap<String, Object>();

        return map;
    }

    //Continue parsing the initial event, inclusion criteria and exclusion criteria with the latest terms.
    @RequestMapping(value = "/continueParsing")
    @ResponseBody
    public Map<String, Object> continueParse(HttpSession httpSession, HttpServletRequest request, String dataset, String nctid, String time, String exc, String inc, String initialEvent,
                                             boolean abb, boolean recon, String obstart, String obend, String daysbefore, String daysafter, String limitto, String explain) {
        String remoteAddr = "";
        if (request != null) {
            remoteAddr = request.getHeader("X-FORWARDED-FOR");
            if (remoteAddr == null || "".equals(remoteAddr)) {
                remoteAddr = request.getRemoteAddr();
            }
        }
        Map<String, Object> map = new HashMap<String, Object>();

        Document doc = (Document) httpSession.getAttribute("allcriteria"); // doc object from runPipeline
        String originalFullResult = this.qfService.getFullResult(doc);
        System.out.println("\n[Getting original doc files]\n");
        JSONArray excArray = JSONArray.fromObject(exc); // These are from front-end
        JSONArray incArray = JSONArray.fromObject(inc);
        JSONArray iniArray = JSONArray.fromObject(initialEvent);

        ObservationConstraint oc = new ObservationConstraint();
        oc.setDaysAfter(Integer.valueOf(daysafter));
        oc.setDaysBefore(Integer.valueOf(daysbefore));
        oc.setLimitTo(limitto);

        if (obstart.length() > 0) {
            oc.setStartDate(obstart);
        } else {
            oc.setStartDate(null);
        }
        if (obend.length() > 0) {
            oc.setEndDate(obend);
        } else {
            oc.setEndDate(null);
        }
        doc.setInitial_event_constraint(oc);
        logger.info("[IP:" + remoteAddr + "][Parsing Results]" + JSONObject.fromObject(doc));

        // Concept extraction update calculation using Levenshtein Distance
        Document updatedDoc = this.ieService.continueTranslateByDoc(doc, iniArray, incArray, excArray);
        // System.out.println("\n[Getting updated doc file from front]\n");
        String fullResult = this.qfService.getFullResult(updatedDoc);
        int conceptDistance = LevenshteinDistance.getDefaultInstance().apply(fullResult, originalFullResult); 
        System.out.println("\nPrint Levenshtein Distance between original and updated criterion:\n" + conceptDistance); // For debugging
        // Concept Reasoning update calculation using Levenshtein Distance
        String originalExplain = (String) httpSession.getAttribute("gpt_explain");
        // System.out.println("\n[Getting original explain from session]\n" + originalExplain);
        // System.out.println("\n[Getting updated explain from front-end]\n" + explain);
        int explainDistance = LevenshteinDistance.getDefaultInstance().apply(originalExplain, explain); // explain is not processable
        System.out.println("\n[Print Levenshtein Distance between original and updated explain]\n" + explainDistance); // For debugging

        // if(conceptDistance < 300 && explainDistance < 100) { // no update from front-end
        if(true) { // no update from front-end
            System.out.println("[There is no update from front, therefore, execute previously generated SQL query]");
            map.put("gpt_explain", explain); // sending original explain to front-end
            
            String sql = (String) httpSession.getAttribute("sql");
            long startTime2 = System.currentTimeMillis();
            JSONArray queryResult = this.qfService.generateReport(sql, dataset); // execute SQL and get the result
            logger.info("[IP:" + remoteAddr + "][End GenerateReport]");        
            long endTime2 = System.currentTimeMillis();
            System.out.println("[[SQL Query Time: " + (endTime2 - startTime2) + "ms]]");        
            map.put("queryResult", queryResult);
            httpSession.setAttribute("queryResult", queryResult);
        } else { // updated from front-end
            System.out.println("\n[Detect criteria & reasoning update from front-end. Re-generate sql based on the updates]\n");

            String sql = this.qfService.generateSQL(fullResult, explain);

            long startTime = System.currentTimeMillis();
            String gpt_explain = this.ieService.gptExplain(sql, fullResult);
            long endTime = System.currentTimeMillis();
            System.out.println("[[GPT Explain Time: " + (endTime - startTime) + "ms]]");
            map.put("gpt_explain", gpt_explain);      
            httpSession.setAttribute("exlpain", gpt_explain);

            // Run query on database
            long startTime2 = System.currentTimeMillis() / 1000;
            JSONArray queryResult = this.qfService.generateReport(sql, dataset); // data indicates the dataset
            logger.info("[IP:" + remoteAddr + "][End GenerateReport]");
            long endTime2 = System.currentTimeMillis() / 1000;
            System.out.println("[[SQL Query Time: " + (endTime2 - startTime2) + " seconds]]");        
            map.put("queryResult", queryResult);
            httpSession.setAttribute("queryResult", queryResult);
        }
        return map;
    }

    @RequestMapping("/runPipeline")
    @ResponseBody
    public Map<String, Object> runWholePipeLine(HttpSession httpSession, HttpServletRequest request, String nctid, String dataset, String initialevent, String inc, String exc, boolean abb, boolean recon, String obstart, String obend, String daysbefore, String daysafter, String limitto) {
        long overallStartTime = System.currentTimeMillis() / 1000;
        //Process of Information Extraction
        Map<String, Object> map = new HashMap<String, Object>();
        String remoteAddr = "";
        if (request != null) {
            remoteAddr = request.getHeader("X-FORWARDED-FOR");
            if (remoteAddr == null || "".equals(remoteAddr)) {
                remoteAddr = request.getRemoteAddr();
            }
        }
        logger.info("[IP:" + remoteAddr + "][Click Parse]");
        logger.info("[IP:" + remoteAddr + "][Initial Event]" + initialevent);
        logger.info("[IP:" + remoteAddr + "][Inclusion Criteria]" + inc);
        logger.info("[IP:" + remoteAddr + "][Exclusion Criteria]" + exc);

        // Information Extraction and Concept Mapping
        Document doc = this.ieService.translateByDoc(initialevent, inc, exc);
       
        // Set the constraints for the document
        ObservationConstraint oc = new ObservationConstraint();
        oc.setDaysAfter(Integer.valueOf(daysafter));
        oc.setDaysBefore(Integer.valueOf(daysbefore));
        oc.setLimitTo(limitto);
        if (obstart.length() > 0) {
            oc.setStartDate(obstart);
        } else {
            oc.setStartDate(null);
        }
        if (obend.length() > 0) {
            oc.setEndDate(obend);
        } else {
            oc.setEndDate(null);
        }
        doc.setInitial_event_constraint(oc);

        List<DisplayCriterion> display_initial_event = this.ieService.displayDoc(doc.getInitial_event());
        List<DisplayCriterion> display_inclusion_criteria = this.ieService.displayDoc(doc.getInclusion_criteria());
        List<DisplayCriterion> display_exclusion_criteria = this.ieService.displayDoc(doc.getExclusion_criteria());        

        logger.info("[IP:" + remoteAddr + "][Parsing Results]" + JSONObject.fromObject(doc));
        httpSession.setAttribute("allcriteria", doc);
        map.put("display_initial_event", display_initial_event);
        map.put("display_include", display_inclusion_criteria);
        map.put("display_exclude", display_exclusion_criteria);        
        
        // Aggregate sentence-level NER results into a single String object
        String fullResult = this.qfService.getFullResult(doc); 
        
        // Generate SQL query 
        String explain = "";
        String sql = this.qfService.generateSQL(fullResult, explain);
        System.out.println("\nPrint initial GPT-genearted sql script\n" + sql);
        httpSession.setAttribute("sql", sql);
                
        // Generate GPT explain for sql
        long startTime = System.currentTimeMillis() / 1000;
        String gpt_explain = this.ieService.gptExplain(sql, fullResult);
        long endTime = System.currentTimeMillis() / 1000;
        System.out.println("[[GPT Explain Time: " + (endTime - startTime) + " seconds]]");
        httpSession.setAttribute("gpt_explain", gpt_explain);
        map.put("gpt_explain", gpt_explain);
        
        // Execute SQL query
        JSONArray queryResult = this.qfService.generateReport(sql, dataset); // data indicates the OMOP dataset
        map.put("queryResult", queryResult);
        httpSession.setAttribute("queryResult", queryResult);

        long overallEndTime = System.currentTimeMillis() / 1000;
        System.out.println("[[[Overall Processing Time: " + (overallEndTime - overallStartTime) + " seconds]]]");

        return map;
    }

    @RequestMapping("/downloadJSON")
    public void saveJSONFile(HttpServletRequest request, HttpServletResponse response, HttpSession httpSession) {
        String remoteAddr = "";
        if (request != null) {
            remoteAddr = request.getHeader("X-FORWARDED-FOR");
            if (remoteAddr == null || "".equals(remoteAddr)) {
                remoteAddr = request.getRemoteAddr();
            }
        }
        logger.info("[IP:" + remoteAddr + "][Download Parsing Results]");
        String jsonResult = (String) httpSession.getAttribute("jsonResult");
        response.setContentType("text/plain");
        String fileName = "Criteria2Query_result";
        try {
            fileName = URLEncoder.encode("Criteria2Query_JSON", "UTF-8");
        } catch (UnsupportedEncodingException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        response.setHeader("Content-Disposition", "attachment; filename=" + fileName + ".json");
        BufferedOutputStream buff = null;
        ServletOutputStream outSTr = null;
        try {
            outSTr = response.getOutputStream();
            buff = new BufferedOutputStream(outSTr);
            buff.write(jsonResult.getBytes("UTF-8"));
            buff.flush();
            buff.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                buff.close();
                outSTr.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @RequestMapping("/downloadSQL")
    public void saveSQLFile(HttpServletRequest request, HttpServletResponse response, HttpSession httpSession) {
        String remoteAddr = "";
        if (request != null) {
            remoteAddr = request.getHeader("X-FORWARDED-FOR");
            if (remoteAddr == null || "".equals(remoteAddr)) {
                remoteAddr = request.getRemoteAddr();
            }
        }
        logger.info("[IP:" + remoteAddr + "][Download Parsing Results]");
        String sqlDialect = request.getParameter("sqlDialect");
        String sqlResult = "";
        if (sqlDialect.equals("PostgreSQL")) {
            // sqlResult = (String) httpSession.getAttribute("postgreSQLResult");
            sqlResult = (String) httpSession.getAttribute("queryResult");
        } else {
            String results = (String) httpSession.getAttribute("sqlResult");
            JSONObject sqljson = new JSONObject();
            sqljson.accumulate("SQL", results);
            if (sqlDialect.equals("MSSQL_Server")) {
                sqljson.accumulate("targetdialect", "sql server");
            }
            sqlResult = JSON2SQL.template2Postgres(sqljson.toString());
            sqlResult = JSON2SQL.fixAge(sqlResult, sqlDialect);
        }
        response.setContentType("text/plain");
        String fileName = "Criteria2Query_result";
        try {
            fileName = URLEncoder.encode("Criteria2Query_SQL", "UTF-8");
        } catch (UnsupportedEncodingException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        response.setHeader("Content-Disposition", "attachment; filename=" + fileName + ".txt");
        BufferedOutputStream buff = null;
        ServletOutputStream outSTr = null;
        try {
            outSTr = response.getOutputStream();
            buff = new BufferedOutputStream(outSTr);
            buff.write(sqlResult.getBytes("UTF-8"));
            buff.flush();
            buff.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                buff.close();
                outSTr.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    @RequestMapping(value = "/queryAgencyPage")
    @ResponseBody
    public JSONObject pageOne(String offset, String limit, HttpServletRequest request,
                              HttpServletResponse response, HttpSession session) {
        Integer offset1 = Integer.parseInt(offset);
        Integer limit1 = Integer.parseInt(limit);
        JSONArray personArray = (JSONArray) session.getAttribute("queryResult");
        int max = 0;
        if (offset1 == 0 && limit1 == -1) {
            max = personArray.size();
        } else {
            if (offset1 + limit1 <= personArray.size()) {
                max = offset1 + limit1;
            } else {
                max = personArray.size();
            }
        }

        JSONArray subset = new JSONArray();
        for (int i = offset1; i < max; i++) {
            subset.add(personArray.get(i));
        }
        JSONObject result = new JSONObject();
        result.accumulate("rows", subset);
        result.accumulate("total", personArray.size());
        return result;
    }


    @RequestMapping(value = "/changeDataset")
    @ResponseBody
    public void changeDataset(HttpSession httpSession, HttpServletRequest request, String dataset) {
        String remoteAddr = "";
        if (request != null) {
            remoteAddr = request.getHeader("X-FORWARDED-FOR");
            if (remoteAddr == null || "".equals(remoteAddr)) {
                remoteAddr = request.getRemoteAddr();
            }
        }

        String sqlResult = (String) httpSession.getAttribute("postgreSQLResult");
        logger.info("[IP:" + remoteAddr + "][Start GenerateReport]");
        JSONArray queryResult = this.qfService.generateReport(sqlResult, dataset);
        logger.info("[IP:" + remoteAddr + "][End GenerateReport]");
        httpSession.setAttribute("queryResult", queryResult);
    }


}
