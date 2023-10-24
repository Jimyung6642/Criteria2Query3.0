package edu.columbia.dbmi.ohdsims.service.impl;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.*;
import java.util.stream.Collectors;

import edu.columbia.dbmi.ohdsims.pojo.*;
import edu.columbia.dbmi.ohdsims.service.IConceptMappingService;
import edu.columbia.dbmi.ohdsims.tool.*;
import edu.columbia.dbmi.ohdsims.util.FileUtil;
import net.sf.json.JSONArray;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.xpath.operations.Bool;
import org.springframework.stereotype.Service;

import edu.columbia.dbmi.ohdsims.service.IInformationExtractionService;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.CoreAnnotations.PartOfSpeechAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.util.CoreMap;
import edu.stanford.nlp.util.Triple;
import jnr.ffi.StructLayout.sa_family_t;
import net.sf.json.JSONObject;

import javax.annotation.Resource;
import javax.servlet.ServletContext;
import javax.swing.*;

import static edu.columbia.dbmi.ohdsims.tool.OHDSIApis.querybyconceptSetid;
import static java.lang.Math.abs;

import com.theokanning.openai.completion.CompletionRequest;
import com.theokanning.openai.completion.chat.*;
import com.theokanning.openai.service.OpenAiService;
import java.time.Duration;
import java.util.regex.*;


@Service("ieService")
public class InformationExtractionServiceImpl implements IInformationExtractionService {

    private static Logger logger = LogManager.getLogger(InformationExtractionServiceImpl.class);
    final static String openaiApiKey = GlobalSetting.openaiApiKey;

    @Resource
    private IConceptMappingService conceptMappingService;

    CoreNLP corenlp = new CoreNLP();
    NERTool nertool = new NERTool();
    NegReTool negtool = new NegReTool();
    LogicAnalysisTool logictool = new LogicAnalysisTool();
    RelExTool reltool = new RelExTool();
    ConceptMapping cptmap = new ConceptMapping();
    ReconTool recontool = new ReconTool();
    NegationDetection nd = new NegationDetection();

    @Override
    public Paragraph translateText(String freetext, boolean include) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Document runIE4Doc(Document doc) {
        // TODO Auto-generated method stub
        return null;
    }

    //Instantiate the Document class, and save the "initial_event", "inclusion_criteria", "exclusion_criteria"
    // after part 1 of the information extraction process, including segmentation and NER.
    @Override
    public Document translateByDoc(String initial_event, String inclusion_criteria, String exclusion_criteria) {
        Document doc = new Document();
        Map<String, Integer> distinctConceptSet = new HashMap<>();        
        long startTime = System.currentTimeMillis();            
        List<Paragraph> initialEventPas = translateByBlockSegNERConceptMapping(initial_event, distinctConceptSet);
        System.out.println("\n[Initial_event succesfully extracted!]\n");
        List<Paragraph> inclusionCriteriaPas = translateByBlockSegNERConceptMapping(inclusion_criteria, distinctConceptSet);
        System.out.println("\n[Inclusion_criteria succesfully extracted!]\n");
        List<Paragraph> exclusionCriteriaPas = translateByBlockSegNERConceptMapping(exclusion_criteria, distinctConceptSet);
        System.out.println("\n[Exclusion_criteria succesfully extracted!]\n");
        long endTime = System.currentTimeMillis();
        System.out.println("\n[[Overall NER & Concept Mapping time:"+(endTime - startTime) + "ms]]"); 

        doc.setInitial_event(initialEventPas);
        doc.setInclusion_criteria(inclusionCriteriaPas);
        doc.setExclusion_criteria(exclusionCriteriaPas);
        return doc;
    }

    public void wholeTextsNegationDetection(List<Paragraph> initialEvent, List<Paragraph> inclusionCriteria, List<Paragraph> exclusionCritiera) {
        List<List<Integer>> allNegateCues = new ArrayList<>();
        List<String> wholeTexts = new ArrayList<>();
        boolean flagNeg = false;
        int initialEventSentNum = 0, inclusionCriteriaSentNum = 0, exclusionCritieraSentNum = 0;
        for (Paragraph pa : initialEvent) {
            List<Sentence> sents = pa.getSents();
            for (Sentence s : sents) {
                List<Integer> cues = s.getNegateCues();
                allNegateCues.add(cues);
                wholeTexts.add("###"+s.getText().trim()+"###");
                if(cues.contains(1) || cues.contains(2)){
                    flagNeg = true;
                }
                initialEventSentNum++;
            }
        }
        for (Paragraph pa : inclusionCriteria) {
            List<Sentence> sents = pa.getSents();
            for (Sentence s : sents) {
                List<Integer> cues = s.getNegateCues();
                allNegateCues.add(cues);
                wholeTexts.add("###"+s.getText().trim()+"###");
                if(cues.contains(1) || cues.contains(2)){
                    flagNeg = true;
                }
                inclusionCriteriaSentNum ++;
            }
        }
        for (Paragraph pa : exclusionCritiera) {
            List<Sentence> sents = pa.getSents();
            for (Sentence s : sents) {
                List<Integer> cues = s.getNegateCues();
                allNegateCues.add(cues);
                wholeTexts.add("###"+s.getText().trim()+"###");
                if(cues.contains(1) || cues.contains(2)){
                    flagNeg = true;
                }
                exclusionCritieraSentNum++;
            }
        }
        System.out.println("Print flagNeg: "+ flagNeg);
        if (flagNeg) {
            //If there is any negation cues in either initial event, inclusion criteria, or exclusion criteria
            List<List<Integer>> negateTags = nd.getNegateTag(allNegateCues, wholeTexts);

            assignNegTag(initialEvent, negateTags.subList(0, initialEventSentNum));
            int iniIncSentNum = initialEventSentNum+inclusionCriteriaSentNum;
            assignNegTag(inclusionCriteria, negateTags.subList(initialEventSentNum, iniIncSentNum));
            assignNegTag(exclusionCritiera, negateTags.subList(iniIncSentNum, iniIncSentNum+exclusionCritieraSentNum));
        }else{
            assignNegTag(initialEvent, null);
            assignNegTag(inclusionCriteria, null);
            assignNegTag(exclusionCritiera, null);
        }
    }


    public void assignNegTag(List<Paragraph> sec, List<List<Integer>> allNegateTags) {
        int i = 0;
        for (Paragraph pa : sec) {
            for (Sentence s : pa.getSents()) {
                List<Integer> negateTags = null;
                if (allNegateTags != null) {
                    negateTags = allNegateTags.get(i);
                    i++;
                }
                List<Term> primary_entities = new ArrayList<>();
                List<Term> attributes = new ArrayList<>();
                for (Term t : s.getTerms()) {
                    if (Arrays.asList(GlobalSetting.primaryEntities).contains(t.getCategorey())) { //If the term belongs to primary entities,
                        // Negation detection
                        boolean ntag = false;
                        if (negateTags != null) {
                            double neg_val = negateTags.subList(t.getIndex().get(0), t.getIndex().get(1)).
                                    stream().mapToDouble(val -> val).average().orElse(0.0);
                            ntag = neg_val>=0.5;
                        }
                        t.setNeg(ntag);
                        primary_entities.add(t);
                    } else if (Arrays.asList(GlobalSetting.atrributes).contains(t.getCategorey())) { //If the term belongs to attributes,
                        attributes.add(t);
                    }
                }
                List<Term> allterms = new ArrayList<Term>();
                allterms.addAll(primary_entities);
                allterms.addAll(attributes);
                s.setTerms(allterms);
                s.setPrimaryEntities(primary_entities);
                s.setAttributes(attributes);
            }

        }

    }

    public List<Paragraph> translateByBlockSegNERConceptMapping(String text, Map<String, Integer> conceptSet) {
        List<Paragraph> spas = new ArrayList<Paragraph>();
        if (text.length() == 0) {
            return spas;
        }
        // Load OpenAI GPT API Key from the config file
        long startTime = System.currentTimeMillis() / 1000;                    
        OpenAiService service = new OpenAiService(openaiApiKey, Duration.ofMinutes(10));        
        final List<ChatMessage> messages = new ArrayList<>();
        // Prompt for pre-defined domain
        final ChatMessage systemMessage = new ChatMessage(ChatMessageRole.SYSTEM.value(), 
                            "Annotate clinical concepts from the given text using the following rules: \n" +
                            // "1) Annotate concepts with the domains 'Demographic', 'Condition', 'Device', 'Cost', 'Procedure', 'Drug', 'Episode', 'Measurement', 'Observation', 'Provider', 'Specimen', 'Visit', 'Value', 'Negation_cue', 'Temporal', and 'Quantity'. If you cannot annotate with the given domains, you can name a new one (e.g., Drug_cycle). \n" +
                            "1) Annotate concepts with the domains 'Demographic', 'Condition', 'Device', 'Procedure', 'Drug', 'Measurement', 'Observation', 'Visit', 'Value', 'Negation_cue', 'Temporal', and 'Quantity'. If you cannot annotate with the given domains, you can name a new one (e.g., Drug_cycle, Visit, Provider, etc.). \n" +
                            "2) Split the concepts as detail as possible. Each concept can be annotated only once with a single domain. \n" +
                            "3) Normalize clinical abbreviation and acronyms and attached behind the original abbreviation with parenthesis. \n" +
                            "4) Return your response under [Annotation] section. \n" +      

                            "Following is not allowed examples: \n" +
                            "1) <Measurement>EGFR <Value>triple postive</Value></Measurement> \n" +
                            "2) <Condition>Hypertension, diabetes, heart failure, and dementia</Condition> \n" +        

                            "Below is allowed examples: \n" +
                            "1) <Measurement>EGFR</Measurement> <Value>triple positive</Value> \n" +
                            "2) <Condition>hypertension</Condition>, <Condition>T2DM (Type 2 Diabetes Mellitus)</Condition>, <Condition>heart failure</Condition>, and <Condition>dementia</Condition> \n" +
                            "3) Patient <Demographic>aged<Demographic> > <Value>65 years old</Value> \n" +
                            "4) <Drug>Metformin</Drug> <Value>500 mg</Value> <Temporal>daily</Temporal> \n" +        
                            
                            "Following is information for each domain: \n" +
                            "1) Condition is events of a Person suggesting the presence of a disease or medical condition stated as a diagnosis, a sign, or a symptom, which is either observed by a Provider or reported by the patient. \n" +
                            "2) Drugs include prescription and over-the-counter medicines, vaccines, and large-molecule biologic therapies. Radiological devices ingested or applied locally do not count as Drugs. \n" +
                            "3) Procedure is records of activities or processes ordered by, or carried out by, a healthcare provider on the patient with a diagnostic or therapeutic purpose. Lab tests are not a procedure, if something is observed with an expected resulting amount and unit then it should be a measurement. \n" +
                            "4) Devices include implantable objects (e.g. pacemakers, stents, artificial joints), medical equipment and supplies (e.g. bandages, crutches, syringes), other instruments used in medical procedures (e.g. sutures, defibrillators) and material used in clinical care (e.g. adhesives, body material, dental material, surgical material). \n" +
                            "5) Measurement contains both orders and results of such Measurements as laboratory tests, vital signs, quantitative findings from pathology reports, etc. OBSERVATION captures clinical facts about a Person obtained in the context of examination, questioning or a procedure. Any data that cannot be represented by any other domains, such as social and lifestyle facts, medical history, family history, etc. are recorded here. \n" +
                            "6) Observations differ from Measurements in that they do not require a standardized test or some other activity to generate clinical fact. Typical observations are medical history, family history, the stated need for certain treatment, social circumstances, lifestyle choices, healthcare utilization patterns, etc. \n" +
                            "7) Demographic can include factors of patient such as age, gender, race, ethnicity, education level, income, occupation, geographic location, marital status, and family size. Age term can be demographic but the specific age criteria should be annotated as value. \n" +
                            "8) Negation_cue includes all information that negates clinical concepts. \n" +
                            "9) Value is the numeric value or string test result of clinical concepts. Typicall values can be the results of Measurements such as Lab test, vital signs, and quantitative findings from pathology reports. It can also be the dosage of drugs, the frequency of drugs, positive/negative of Gene test or lab test, the duration of drugs or numeric criteria of age, weight, height, etc.");
        // Prompt for GPT-defined domain
        //final ChatMessage systemMessage = new ChatMessage(ChatMessageRole.SYSTEM.value(), "Annotate clinical concepts from the given text using the following rules: 1) Split the concepts as detail as possible. Each concept can be annotated only once with a single domain. 2) Normalize clinical abbreviation and acronyms and attached behind the original abbreviation with parenthesis. 3) Identify the relation, temporal, negation, and logic between the concepts. 4) Divide your response into two sections, [Annotation] and [Explanation]. [Annotation] section is the sentence with the annotation tags and the [Explain] is the your explanation of the annotation tags with numbers. Following is not allowed examples: 1) <Gene_test>EGFR <Result>triple postive</Result></Gene_test> 2) <Diagnosis>Hypertension, diabetes, heart failure, and dementia</Diagnosis> Below is allowed examples: 1) <Gene_test>EGFR</Gene_test> <Result>triple positive</Result> 2) <Diagnosis>hypertension</Diagnosis>, <Diagnosis>T2DM (Type 2 Diabetes Mellitus)</Diagnosis>, <Diagnosis>heart failure</Diagnosis>, and <Diagnosis>dementia</Diagnosis> 3) Patient <Demographic>aged<Demographic> > <Value>65 years old</Value> 4) <Drug>Metformin</Drug> <Dosage>500 mg</Dosage> <Temporal>daily</Temporal> ");
        
        final ChatMessage userMessage = new ChatMessage(ChatMessageRole.USER.value(), text); 
        messages.add(systemMessage);
        messages.add(userMessage);

        ChatCompletionRequest chatCompletionRequest = ChatCompletionRequest
                                                            .builder()
                                                            .model("gpt-4") //gpt-4 gpt-3.5-turbo
                                                            .messages(messages)
                                                            .n(1)
                                                            .temperature(0.0)
                                                            .build();
        try {
            List<ChatCompletionChoice> choices = service.createChatCompletion(chatCompletionRequest).getChoices();
            text = choices.get(0).getMessage().getContent().toString();
        } catch(Exception e){
            System.out.println("\nError in OpenAI API: \n" + e.getMessage() + "\n");
        }
        long endTime = System.currentTimeMillis() / 1000;   
        System.out.println("\nNER API responding time:"+(endTime - startTime) + " seconds"); 
        String annotation = text.replace("[Annotation]", "");        
        
        // Run NER per each paragraph
        String[] pas = annotation.split("\n");
        for (String p : pas) {
            if (p.trim().isEmpty()) {
                continue;
            }
            Paragraph pa = new Paragraph();
            List<String> block_text = corenlp.splitParagraph(p);//split p text into sentences.
            List<Sentence> sents = new ArrayList<Sentence>();
            // NER, relation, negation, logic are operated against sentence level
            for (String s : block_text) {
                String gptResults = s;
                String pattern = "<([\\s\\S]*?)>|</([\\s\\S]*?)>";
                s = s.replaceAll(pattern, " ");
                s = s.replaceAll("  ", " ");
                Sentence sent = new Sentence(" " + s + " "); 
                Object[] items = nertool.formulateNerResult(sent.getText(), gptResults);//Extract entities, and save them as term objects.
                List<Term> terms = (List<Term>) items[0];
                List negateCues = (List) items[1];                
                //boolean containNeg = (boolean) items[2];

                String display = "";
                try {
                    display = nertool.trans4display(sent.getText(), terms, conceptSet);
                    //Concept mapping;
                    //Translate the sentence with the designated form of terms for display.
                } catch (Exception ex) {
                    logger.error("Error in trans4display: " + ex.getMessage());
                }
                //String display = nertool.trans2Html(crf_results);
                sent.setTerms(terms);
                sent.setDisplay(display);
                //System.out.println("Print display:\n" + sent.getDisplay() + "\n");
                sent.setNegateCues(negateCues);
                sents.add(sent);
            }
            pa.setSents(sents);
            //logger.info(JSONObject.fromObject(pa));
            spas.add(pa);
        }
        return spas;
    }

    public void translateByBlockRelLogExtract(List<Paragraph> sec) {
        for (Paragraph pa : sec) {
            for (Sentence sent : pa.getSents()) {
                List<Triple<Integer, Integer, String>> relations = new ArrayList<Triple<Integer, Integer, String>>();
                List<Term> primaryEntities = sent.getPrimaryEntities();
                List<Term> attributes = sent.getAttributes();
                for (Term t : primaryEntities) {
                    for (Term a : attributes){
                        String rel = "no_relation";
                        boolean relflag = false;
                        relflag = true;
                        if (relflag == true && a.getCategorey().equals("Value")) {
                            rel = "has_value";
                        }

                        //if (relflag == true && a.getCategorey().equals("Temporal")) {
                        if (a.getCategorey().equals("Temporal")) {
                            rel = "has_temporal";
                        }

                        Triple<Integer, Integer, String> triple = new Triple<Integer, Integer, String>(t.getTermId(),a.getTermId(), rel);

                        if (triple.third().equals("no_relation") == false) {
                            relations.add(triple);
                        }

                    }
                }
                //relation revision
                relations = reltool.relsRevision(sent.getTerms(), relations, "has_temporal");
                relations = reltool.relsRevision(sent.getTerms(), relations, "has_value");
                //It revises the "has_temporal" relation.
                // Among all term2s that have this relation with the same term1,
                // it only saves the relation between term1 and term2 which have the shortest distance.
                sent.setRelations(relations);
                //Logistic Extraction
                List<LinkedHashSet<Integer>> logic_groups = logictool.ddep(sent.getText(), primaryEntities);
                //Detect logic "or" relation between entities, and save groups of entities which are in "or" relation into a list.
                sent.setLogic_groups(logic_groups);
            }
        }
    }

    public String gptExplain(String sql, String fullResult) {
        List<DisplayCriterion> displaycriteria = new ArrayList<DisplayCriterion>();        
        // Extract HTML tags
        Pattern pattern = Pattern.compile("<[^>]+concept-id=\"(\\d+)\"[^>]*>(.*?)</[^>]+>");
        Matcher matcher = pattern.matcher(fullResult);
        StringBuilder concept = new StringBuilder();
        while (matcher.find()) {            
            String conceptName = matcher.group(0);
            concept.append(conceptName).append("\n");
        }
        String input = "SQL:\n" + sql + "\n" + "\nConcept List:\n" + concept.toString();
        System.out.println("\nPrint input data used for query" + input + "\n");

        try {
            OpenAiService service = new OpenAiService(openaiApiKey, Duration.ofMinutes(20));        
            final List<ChatMessage> messages = new ArrayList<>();        
            final ChatMessage systemMessage = new ChatMessage(ChatMessageRole.SYSTEM.value(), 
                // "You need to explain reasoning of eligible criteria from a given query with the following rules. \n" +
                "Explain logics (AND/OR), Temporal, Value, and negation relations between clinical concepts from a given query with the following rules. \n" +
                "1. The query is based on OMOP-CDM version 5.3.2 \n" +
                "2. You can identify concept-name of concept-id using the provided list which consists of [data-entity], [concept-id], and [concept-name]. Concept-name is located behine concept-id and in front of <b><i>. Neglact the information behind <b><i> tag" +
                "3. If you find the unused concepts, then explain why they are not used in the query. One of the possible reason is the incluson of disease hierarchy. \n" +
                "4. Return your answer with numbers and highlight identified clinical concepts (concept-names) with \"\".\n" +
                "5. Your answer should very clearly define the relations and logics with narrative explanation. \n" +
                "6. Explain with [concept-name] and without [concept-id] to make answer more easy to read by clinicians. \n"
                );
            final ChatMessage userMessage = new ChatMessage(ChatMessageRole.USER.value(), input);
            messages.add(systemMessage);
            messages.add(userMessage);
    
            ChatCompletionRequest chatCompletionRequest = ChatCompletionRequest
                                                                .builder()
                                                                .model("gpt-4") //gpt-4
                                                                .messages(messages)
                                                                .n(1)
                                                                .temperature(0.0)
                                                                .build();
            List<ChatCompletionChoice> gpt_explain = service.createChatCompletion(chatCompletionRequest).getChoices();        
            String gptExplain = gpt_explain.get(0).getMessage().getContent().toString();
            System.out.println("\nPrint GPT Explain:\n" + gptExplain + "\n");        
            
            // Return the result as single String explanation
            return gptExplain;

            // // Split the full result into paragraphs
            // String[] titles = {"", "Initial_event", "Inclusion_criteria", "Exclusion_criteria"};
            // String[] parts = gptExplain.split("\\[Initial_event\\]|\\[Inclusion_criteria\\]|\\[Exclusion_criteria\\]");
            // for (int i = 1; i < parts.length; i++) {
            //     DisplayCriterion d = new DisplayCriterion();
            //     String part = parts[i].trim();
            //     String title = i < titles.length ? titles[i] : "Unknown";
            //     d.setDatabase(title);
            //     d.setId(i);
            //     d.setCriterion(part.isEmpty() ? "Nothing to explain" : part);
            //     displaycriteria.add(d);
            // }
            // return displaycriteria;
        } catch(Exception e){
            // System.out.println("\nError in GPT Explain: \n" + e.getMessage());
            // return displaycriteria;            
            System.out.println("Error in gpt explain");
            return "";
        }
    }

    @Override
    public List<DisplayCriterion> displayDoc(List<Paragraph> ps) {
        // TODO Auto-generated method stub
        List<DisplayCriterion> displaycriteria = new ArrayList<DisplayCriterion>();
        int i = 1;
        for (Paragraph p : ps) {
            boolean ehrstatus = false;
            DisplayCriterion d = new DisplayCriterion();
            StringBuffer sb = new StringBuffer();
            for (Sentence s : p.getSents()) {
                sb.append(s.getDisplay());
                for (Term t : s.getTerms()) {
                    if (Arrays.asList(GlobalSetting.primaryEntities).contains(t.getCategorey())) {
                        ehrstatus = true;
                    }
                }
            }
            d.setCriterion(sb.toString());
            d.setId(i++);
            d.setEhrstatus(ehrstatus);
            displaycriteria.add(d);
        }
        return displaycriteria;
    }

    @Override
    public Document patchIEResults(Document doc) {
        // TODO Auto-generated method stub
        //If initial_event exists,
        if (doc.getInitial_event() != null) {
            List<Paragraph> originalp = doc.getInitial_event();
            originalp = patchDocLevel(originalp);//Add "Demographic" term together with the "has_value" relation.
            doc.setInitial_event(originalp);
        }
        //If inclusion_criteria exists,
        if (doc.getInclusion_criteria() != null) {
            List<Paragraph> originalp = doc.getInclusion_criteria();
            originalp = patchDocLevel(originalp);//Add "Demographic" term together with the "has_value" relation.
            doc.setInclusion_criteria(originalp);
        }
        //If exclusion_criteria exists,
        if (doc.getExclusion_criteria() != null) {
            List<Paragraph> originalp = doc.getExclusion_criteria();
            originalp = patchDocLevel(originalp);//Add "Demographic" term together with the "has_value" relation.
            doc.setExclusion_criteria(originalp);
        }
        return doc;
    }


    @Override
    public Document reconIEResults(Document doc) {
        // TODO Auto-generated method stub
        if (doc.getInitial_event() != null) {
            List<Paragraph> originalp = doc.getInitial_event();
            originalp = reconOnDocLevel(originalp);
            doc.setInitial_event(originalp);
        }
        if (doc.getInclusion_criteria() != null) {
            List<Paragraph> originalp = doc.getInclusion_criteria();
            originalp = reconOnDocLevel(originalp);
            doc.setInclusion_criteria(originalp);
        }
        if (doc.getExclusion_criteria() != null) {
            List<Paragraph> originalp = doc.getExclusion_criteria();
            originalp = reconOnDocLevel(originalp);
            doc.setExclusion_criteria(originalp);
        }
        return doc;
    }

    // term-level calibration
    public List<Paragraph> reconOnDocLevel(List<Paragraph> originalp) {
        for (Paragraph p : originalp) {
            if (p.getSents() != null) {
                for (Sentence s : p.getSents()) {
                    if (s.getTerms() != null) {
                        for (int i = 0; i < s.getTerms().size(); i++) {
                            if (s.getTerms().get(i).getCategorey().equals("Condition")
                                    || s.getTerms().get(i).getCategorey().equals("Drug")
                                    || s.getTerms().get(i).getCategorey().equals("Measurement")
                                    || s.getTerms().get(i).getCategorey().equals("Procedure")
                                    || s.getTerms().get(i).getCategorey().equals("Observation")) {//If the term belongs to the concept set
                                String text = s.getTerms().get(i).getText();

                                if (recontool.isCEE(text)) {//If "and", ",", "or", or "/" exists in the text,

                                    Term t = s.getTerms().get(i);
                                    String category = t.getCategorey();
                                    String entity = t.getText();
                                    Integer start_index = t.getStart_index();
                                    Integer end_index = t.getEnd_index();
                                    List<String> concepts = recontool.resolve(t.getText());
                                    int count = 0;
                                    for (String c : concepts) {
                                        //System.out.println("=>"+c);
                                        Term ret = new Term();
                                        Integer newtId = t.getTermId() + 100 + count;
                                        ret.setTermId(newtId);
                                        ret.setText(c);
                                        ret.setNeg(t.isNeg());
                                        ret.setCategorey(t.getCategorey());
                                        ret.setStart_index(t.getStart_index());
                                        ret.setEnd_index(t.getEnd_index());
                                        s.getTerms().add(ret);
                                        count++;
                                    }


                                    s.getTerms().remove(i);
                                }
                            }

                        }
                    }
                }
            }
        }
        return originalp;
    }

    //Patch the term's category, especially for the category "Temporal".
    public List<Term> patchTermLevel(List<Term> terms) {
        for (int i = 0; i < terms.size(); i++) {
            List<String> lemmas = corenlp.getLemmasList(terms.get(i).getText());//Lemmatize the text of the term and turn it into a list of string.
            if ((lemmas.contains("day") || lemmas.contains("month") || lemmas.contains("year")) && (lemmas.contains("old") == false) && (lemmas.contains("/") == false)) {
                if (i > 0 && terms.get(i - 1).getCategorey().equals("Demographic") == false) {
                    terms.get(i).setCategorey("Temporal");
                }

            }
        }
        return terms;
    }


    // term-level calibration
    public List<Paragraph> patchDocLevel(List<Paragraph> originalp) {
        for (Paragraph p : originalp) {
            if (p.getSents() != null) {
                for (Sentence s : p.getSents()) {
                    if (s.getTerms() != null) {
                        for (int i = 0; i < s.getTerms().size(); i++) {
                            if (s.getTerms().get(i).getCategorey().equals("Value")) {
                                String text = s.getTerms().get(i).getText();
                                List<String> lemmas = corenlp.getLemmasList(text);
                                if (lemmas.contains("old") || lemmas.contains("young") || lemmas.contains("older")
                                        || lemmas.contains("younger")) {//If this "Value" term contains "old", "young", etc. after lemmatization.
                                    // if there is no age in this sentence.
                                    if (hasDemoAge(s.getTerms()) == false) {
                                        Term t = new Term();
                                        t.setCategorey("Demographic");
                                        t.setStart_index(-1);
                                        t.setEnd_index(-1);
                                        t.setNeg(false);
                                        t.setText("age");
                                        Integer assignId = s.getTerms().size();
                                        t.setTermId(assignId);
                                        s.getTerms().add(t);//Add a term "age" into the term list of this sentence.
                                        s.getRelations().add(new Triple<Integer, Integer, String>(assignId,
                                                s.getTerms().get(i).getTermId(), "has_value"));//Add a "has_value" relation between this "age" term and the "Value" term.
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        return originalp;
    }

    //Check if "age" is in the terms after lemmatization.
    public boolean hasDemoAge(List<Term> terms) {
        for (Term t : terms) {
            List<String> lemmas = corenlp.getLemmasList(t.getText());
            if (lemmas.get(0).equals("age")) {
                return true;
            }
        }
        return false;
    }

    public Document abbrExtensionByDoc(Document doc) {
        // TODO Auto-generated method stub

        if (doc.getInitial_event() != null) {
            List<Paragraph> originalp = doc.getInitial_event();
            originalp = abbrExtension(originalp);
            doc.setInitial_event(originalp);
        }
        if (doc.getInclusion_criteria() != null) {
            List<Paragraph> originalp = doc.getInclusion_criteria();
            originalp = abbrExtension(originalp);
            doc.setInclusion_criteria(originalp);
        }
        if (doc.getExclusion_criteria() != null) {
            List<Paragraph> originalp = doc.getExclusion_criteria();
            originalp = abbrExtension(originalp);
            doc.setExclusion_criteria(originalp);
        }
        return doc;
    }

    public List<Paragraph> abbrExtension(List<Paragraph> originalp) {
        for (Paragraph p : originalp) {
            if (p.getSents() != null) {
                for (Sentence s : p.getSents()) {
                    if (s.getTerms() != null) {
                        for (int i = 0; i < s.getTerms().size(); i++) {
                            if (isAcronym(s.getTerms().get(i).getText())) {//If the term is an acronym
                                String extendphrase = cptmap.extendByUMLS(s.getTerms().get(i).getText());
                                s.getTerms().get(i).setText(extendphrase);
                            }
                        }
                    }
                }
            }
        }
        return originalp;
    }


    public boolean isAcronym(String word) {
        // if the word is less than three letters.
        if (word.length() < 3) {
            return true;
        } else {
            if (word.indexOf(" ") == -1) {
                for (int i = 0; i < word.length(); i++) {
                    if (Character.isDigit(word.charAt(i))) {
                        return true;
                    }
                }
            }
            // if all upper case
            if (Character.isUpperCase(word.charAt(1))) {
                return true;
            }
        }
        // if there is a number in the word

        return false;
    }

    @Override
    public List<String> getAllInitialEvents(Document doc) {
        List<String> initevent = new ArrayList<String>();
        List<Paragraph> initial_events = doc.getInitial_event();
        if (initial_events != null) {
            for (Paragraph p : initial_events) {
                List<Sentence> sents = p.getSents();
                if (sents != null) {
                    for (Sentence s : sents) {
                        List<Term> terms = s.getTerms();
                        if (terms != null) {
                            for (Term t : terms) {
                                if (Arrays.asList(GlobalSetting.conceptSetDomains).contains(t.getCategorey())) {
                                    initevent.add(t.getText());
                                }
                            }
                        }
                    }
                }
            }
        }
        return initevent;
    }

    public Document continueTranslateByDoc(Document doc, JSONArray iniResult, JSONArray incResult, JSONArray excResult) {
        Document newDoc = new Document();
        Map<String, Integer> distinctConceptSet = new HashMap<>();
        List<Paragraph> initialEventPas = translateByBlockNer(doc.getInitial_event(), iniResult, distinctConceptSet);
        List<Paragraph> inclusionCriteriaPas = translateByBlockNer(doc.getInclusion_criteria(), incResult, distinctConceptSet);
        List<Paragraph> exclusionCriteriaPas = translateByBlockNer(doc.getExclusion_criteria(), excResult, distinctConceptSet);
        //wholeTextsNegationDetection(initialEventPas, inclusionCriteriaPas, exclusionCriteriaPas);
        //translateByBlockRelLogExtract(initialEventPas);
        //translateByBlockRelLogExtract(inclusionCriteriaPas);
        //translateByBlockRelLogExtract(exclusionCriteriaPas);
        newDoc.setInitial_event(initialEventPas);
        newDoc.setInclusion_criteria(inclusionCriteriaPas);
        newDoc.setExclusion_criteria(exclusionCriteriaPas);
        return newDoc;
    }

    //It parses the text in a block with the process of recognizing the latest terms, detecting negations, and extracting relations.
    public List<Paragraph> translateByBlockNer(List<Paragraph> pas, JSONArray result, Map<String, Integer> conceptSetMap) {
        List<Paragraph> newPas = new ArrayList<>();
        if (result.size() == 0) {
            return newPas;
        }
        for (int j = 0; j < result.size(); j++) {
            JSONObject criterion = JSONObject.fromObject(result.get(j));
            String pasResult = criterion.getString("criterion"); // updated criterion
            // System.out.println("\nUpdated criteria:\n " + pasResult);      // Debug purpose. need to be deleted      
            Integer id = criterion.getInt("id") - 1;//pas indexes from 0; id of criterion indexes from 1;
            List<String> sensResult = corenlp.splitParagraph(pasResult);//Split a paragraph into sentences.
            Paragraph pa = pas.get(id);            
            List<Sentence> newSents = new ArrayList<>();
            for (int k = 0; k < sensResult.size(); k++) {
                String sResult = sensResult.get(k);
                sResult = sResult.replaceAll("-LRB-", "(");
                sResult = sResult.replaceAll("-RRB-", ")");
                sResult = sResult.replaceAll("-LSB-", "[");
                sResult = sResult.replaceAll("-RSB-", "]");
                sResult = sResult.replaceAll("-LCB-", "{");
                sResult = sResult.replaceAll("-RCB-", "}");

                Sentence sent = pa.getSents().get(k);
                String s = sent.getText();
                Object[] items = nertool.formulateTerms(s, sResult, conceptSetMap);//Get all terms in the sentence.
                List<Term> terms = (List<Term>) items[0];
                List negateCues = (List) items[1];

                sent.setTerms(terms);
                sent.setNegateCues(negateCues);
                sent.setDisplay(sResult); // 23.05.23 jimyung park
                newSents.add(sent);
            }
            pa.setSents(newSents);
            newPas.add(pa);
        }
        return newPas;
    }


    public Boolean compareTerms(List<Term> newTerms, List<Term> oldTerms, String userId, Long lastAccessedTime, String filePath) {
        Boolean match = true;
        int i = 0, j = 0;
        String record = "";
        if (newTerms.size() > 0 && oldTerms.size() > 0) {
            while (i + j < newTerms.size() + oldTerms.size() - 1) {
                if (newTerms.get(i).getStart_index().equals(oldTerms.get(j).getStart_index()) &&
                        newTerms.get(i).getEnd_index().equals(oldTerms.get(j).getEnd_index())) {
                    if (!newTerms.get(i).getCategorey().equals(oldTerms.get(j).getCategorey())) {
                        // System.out.println("update" + newTerms.get(i).getText());
                        record = record + userId + "," + lastAccessedTime.toString() + ",update," + oldTerms.get(j).getText() + "," +
                                GlobalSetting.domainAbbrMap.get(oldTerms.get(j).getCategorey()) + "," + oldTerms.get(j).getConceptName() + "," +
                                GlobalSetting.domainAbbrMap.get(newTerms.get(i).getCategorey()) + "," + newTerms.get(i).getConceptName() + "\n";
                        match = false;
                    } else {
                        if (Arrays.asList(GlobalSetting.conceptSetDomains).contains(newTerms.get(i).getCategorey())) {
                            if (!newTerms.get(i).getConceptId().equals(oldTerms.get(j).getConceptId())) {
                                //System.out.println("update" + newTerms.get(i).getText());
                                record = record + userId + "," + lastAccessedTime.toString() + ",update," + oldTerms.get(j).getText() + "," +
                                        GlobalSetting.domainAbbrMap.get(oldTerms.get(j).getCategorey()) + "," + oldTerms.get(j).getConceptName() + "," +
                                        GlobalSetting.domainAbbrMap.get(newTerms.get(i).getCategorey()) + "," + newTerms.get(i).getConceptName() + "\n";
                                match = false;
                            }
                        }
                    }
                    i++;
                    j++;
                } else {
                    match = false;
                    if (newTerms.get(i).getStart_index() < oldTerms.get(j).getStart_index() &&
                            newTerms.get(i).getEnd_index() < oldTerms.get(j).getStart_index()) {
                        // System.out.println("add" + newTerms.get(i).getText());
                        record = record + userId + "," + lastAccessedTime.toString() + ",add," + newTerms.get(i).getText() + "," +
                                "," + "," +
                                GlobalSetting.domainAbbrMap.get(newTerms.get(i).getCategorey()) + "," + newTerms.get(i).getConceptName() + "\n";
                        i++;
                    } else if (newTerms.get(i).getStart_index() < oldTerms.get(j).getStart_index() &&
                            newTerms.get(i).getEnd_index() >= oldTerms.get(j).getStart_index()) {
                        //System.out.println("delete" + oldTerms.get(j).getText());
                        record = record + userId + "," + lastAccessedTime.toString() + ",delete," + oldTerms.get(j).getText() + "," +
                                GlobalSetting.domainAbbrMap.get(oldTerms.get(j).getCategorey()) + "," + oldTerms.get(j).getConceptName() + "," +
                                "," + "\n";
                        // System.out.println("add" + newTerms.get(i).getText());
                        record = record + userId + "," + lastAccessedTime.toString() + ",add," + newTerms.get(i).getText() + "," +
                                "," + "," +
                                GlobalSetting.domainAbbrMap.get(newTerms.get(i).getCategorey()) + "," + newTerms.get(i).getConceptName() + "\n";
                        i++;
                        j++;
                    } else if (newTerms.get(i).getStart_index() >= oldTerms.get(j).getStart_index() &&
                            newTerms.get(i).getStart_index() <= oldTerms.get(j).getEnd_index()) {
                        // System.out.println("delete" + oldTerms.get(j).getText());
                        record = record + userId + "," + lastAccessedTime.toString() + ",delete," + oldTerms.get(j).getText() + "," +
                                GlobalSetting.domainAbbrMap.get(oldTerms.get(j).getCategorey()) + "," + oldTerms.get(j).getConceptName() + "," +
                                "," + "\n";
                        // System.out.println("add" + newTerms.get(i).getText());
                        record = record + userId + "," + lastAccessedTime.toString() + ",add," + newTerms.get(i).getText() + "," +
                                "," + "," +
                                GlobalSetting.domainAbbrMap.get(newTerms.get(i).getCategorey()) + "," + newTerms.get(i).getConceptName() + "\n";
                        i++;
                        j++;
                    } else if (newTerms.get(i).getStart_index() >= oldTerms.get(j).getEnd_index()) {
                        // System.out.println("delete" + oldTerms.get(j).getText());
                        record = record + userId + "," + lastAccessedTime.toString() + ",delete," + oldTerms.get(j).getText() + "," +
                                GlobalSetting.domainAbbrMap.get(oldTerms.get(j).getCategorey()) + "," + oldTerms.get(j).getConceptName() + "," +
                                "," + "\n";
                        j++;
                    }
                }
                if (i == newTerms.size() && j < oldTerms.size()) {
                    match = false;
                    for (int k = j; k < oldTerms.size(); k++) {
                        //System.out.println("delete" + oldTerms.get(k).getText());
                        record = record + userId + "," + lastAccessedTime.toString() + ",delete," + oldTerms.get(k).getText() + "," +
                                GlobalSetting.domainAbbrMap.get(oldTerms.get(k).getCategorey()) + "," + oldTerms.get(k).getConceptName() + "," +
                                "," + "\n";
                    }
                    break;
                } else if (j == oldTerms.size() && i < newTerms.size()) {
                    match = false;
                    for (int k = i; k < newTerms.size(); k++) {
                        //System.out.println("add" + newTerms.get(k).getText());
                        record = record + userId + "," + lastAccessedTime.toString() + ",add," + newTerms.get(k).getText() + "," +
                                "," + "," +
                                GlobalSetting.domainAbbrMap.get(newTerms.get(k).getCategorey()) + "," + newTerms.get(k).getConceptName() + "\n";
                    }
                    break;
                }
            }
        } else if (newTerms.size() > 0 && oldTerms.size() == 0) {
            match = false;
            for (int k = i; k < newTerms.size(); k++) {
                //System.out.println("add" + newTerms.get(k).getText());
                record = record + userId + "," + lastAccessedTime.toString() + ",add," + newTerms.get(i).getText() + "," +
                        "," + "," +
                        GlobalSetting.domainAbbrMap.get(newTerms.get(i).getCategorey()) + "," + newTerms.get(i).getConceptName() + "\n";
            }
        } else if (oldTerms.size() > 0 && newTerms.size() == 0) {
            match = false;
            for (int k = j; k < oldTerms.size(); k++) {
                //System.out.println("delete" + oldTerms.get(k).getText());
                record = record + userId + "," + lastAccessedTime.toString() + ",delete," + oldTerms.get(j).getText() + "," +
                        GlobalSetting.domainAbbrMap.get(oldTerms.get(j).getCategorey()) + "," + oldTerms.get(j).getConceptName() + "," +
                        "," + "\n";
            }
        }
        // System.out.println(record);
        FileUtil.add2File(filePath, record);
        return match;
    }


}
