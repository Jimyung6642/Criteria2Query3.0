package edu.columbia.dbmi.ohdsims.util;

import edu.columbia.dbmi.ohdsims.pojo.GlobalSetting;
import edu.columbia.dbmi.ohdsims.tool.JSON2SQL;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.json.CDL;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static edu.columbia.dbmi.ohdsims.pojo.GlobalSetting.ohdsi_api_base_url;

import com.theokanning.openai.completion.CompletionRequest;
import com.theokanning.openai.completion.chat.*;
import com.theokanning.openai.service.OpenAiService;
import java.time.Duration;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class SQLUtil {
    final static String url1K = GlobalSetting.databaseURL1K;
    final static String url5pct = GlobalSetting.databaseURL5pct;
    final static String user = GlobalSetting.databaseUser;
    final static String password = GlobalSetting.databasePassword;
    final static String openaiApiKey = GlobalSetting.openaiApiKey;

    public static void main(String[] args) throws IOException {
        // TODO Auto-generated method stub
        //Read the JSON file

        for(int i = 1; i<2; i++){
            String pathname = "./test cases/JSON/JSON"+ i+".txt";
            File filename = new File(pathname);
            InputStreamReader reader = new InputStreamReader(
                    new FileInputStream(filename));
            BufferedReader br = new BufferedReader(reader);
            StringBuffer lineBuffer = new StringBuffer();
            String line = null;
            while ((line = br.readLine()) != null) {
                lineBuffer.append(line);
            }
            String jsonTxt = new String(lineBuffer);
            JSONObject jsonObject = JSONObject.fromObject(jsonTxt);
            JSONObject expression = new JSONObject();
            expression.accumulate("expression", jsonObject);
            //System.out.println("expressionstr="+expression);

            //APIS in Online WebAPI
            //generate SQL template
            long startTime=System.currentTimeMillis();
            String results = HttpUtil.doPost(ohdsi_api_base_url + "cohortdefinition/sql", expression.toString());
            JSONObject resultjson = JSONObject.fromObject(results);
            //SQL template -> different SQLs
            JSONObject sqljson = new JSONObject();
            sqljson.accumulate("SQL", resultjson.get("templateSql"));
            sqljson.accumulate("targetdialect", "postgresql");
            results = HttpUtil.doPost(ohdsi_api_base_url + "sqlrender/translate", sqljson.toString());
            resultjson = JSONObject.fromObject(results);
            String sqlResult = (String) resultjson.get("targetSQL");
            //System.out.println(sqlResult);
            long endTime=System.currentTimeMillis();
            System.out.println("time(Online WebAPI): "+(endTime-startTime)+"ms");

            //APIs in local WebAPI by ZC
            //generate SQL template
            startTime=System.currentTimeMillis();
            String results1 = JSON2SQL.SQLTemplate(expression.toString());
            //SQL template -> different SQLs
            JSONObject sqljson1 = new JSONObject();
            sqljson1.accumulate("SQL", results1);
            sqljson1.accumulate("targetdialect", "postgresql");
            results1 = JSON2SQL.template2Postgres(sqljson1.toString());
            endTime=System.currentTimeMillis();
            System.out.println("time(Local WebAPI): "+(endTime-startTime)+"ms");
            String dataset = "SynPUF 1K dataset";
            executeSQL(results1, dataset);
        }



    }

    public static String cleanSQL(String possql) {
        String sql = possql.replace("@vocabulary_database_schema", "public");
        sql = sql.replace("@cdm_database_schema", "public");
        sql = sql.replace("@target_database_schema", "public");
        sql = sql.replace("@target_cohort_table", "cohort");
        sql = sql.replace("@target_cohort_id", "1");
        int x = sql.indexOf("DELETE FROM");
        //System.out.println("--->" + x);
        sql = sql.substring(0, x);
        return sql;
    }

    public static String generateSqlScript(String annotation, String explain) {
        // Generate SQL query using OpenAI API
        OpenAiService service = new OpenAiService(openaiApiKey, Duration.ofMinutes(20));
        final List<ChatMessage> messages = new ArrayList<>();
        // For pre-defined domain concept mapping
        final ChatMessage systemMessage = new ChatMessage(ChatMessageRole.SYSTEM.value(), 
        "Generate a postgreSQL query from the given text while following the rules: \n" +
        "1. Target database is OMOP-CDM version 5.3.2. \n" +
        "2. Use CTE and add semi-colon after sql statement.\n" +
        "3. Extract 'person_id', 'gender_concept_id', 'year_of_birth', 'month_of_birth', 'day_of_birth', and 'race_concept_id' from [PERSON] table. 'year_of_birth', 'month_of_birth', and 'day_of_birth' columns are string.\n" +
        "4. Create temp table named [final_cohort] and insert the query result into [final_cohort]. \n" +
        "5. The given text consists of [Initial_event], [Inclusion_criteria], [Exclusion_criteria]. Sentences with HTML mark tags located under each section. [Initial_event] will be entry of cohort. However, if htere is nothing under [Initial_event] section, then [Inclusion_criteria] will be [Initial_event] and all the sentences under [Inclusion_criteria] will be used for cohort entry event. \n" +
        "6. Patients who satisfies [Inclusion_criteria] can only be included in the cohort and exclude patients who satisfied [Exclusion_criteria]. \n" +
        "7. Sentences under each section has HTML mark tags, i.e., '<mark data-entity=\"[domain]\" concept-id=\"[concept_id]\">[token]</mark>'. Use the [concept_id] in the tag if it exists. \n" +
        "8. Annotated concepts without [concept-id] (e.g., [value], [temporal], [negation_cue]) are dependant to other concepts with [concept-id]. Consider the relationship between these concepts while generating the queries. For example, '<mark data-entity=\"drug\" concept-id=\"[concept_id]\">metformin</mark> is <mark data-entity=\"negation_cue\"not precribed</mark> for <mark data-entity=\"temporal\"6 month</mark>' means 'negation_cue' and 'temporal' should be considered in query with metformin. One exception is [demographic] which is related to [PERSON] table\n" +
        "9. Include descendant concepts of the [concept_id]. For example, [select * from CONDITION_OCCURRENCE where condition_concept_id in (select descendant_concept_id from concept_ancestor where ancestor_concept_id = 'target_concept_id')]. The concept_id columns of CDM tables are '[domain]_concept_id'. \n" +
        "10. Use all concepts in the given text to generate the query and use subquery for each [concept-id]. Do not query [concept-id]s together unless there is any comments defining concept relationships such as  'or', 'one of', 'any of', 'at leat'. \n" +
        "11. Consider clinician provided comments in the given text, if exists. It has information of logic, negation, relation of concepts, which is crucial generating query. \n" +
        "Hint: [domain] in the tag indicates the clinical domain of the concept and they are directly mapped to the OMOP-CDM tables. Left side is the [domain] in given text and right side is the name of CDM tables. \n" +
        "1) [demographic] = [PERSON] 2) [condition] = [CONDITION_OCCURRENCE] 3) [drug] = [DRUG_EXPOSURE] 4) [procedure] = [PROCEDURE_OCCURRENCE] 5) [device] = [DEVICE_EXPOSURE] 6) [observation] = [OBSERVATION] 7) [measurement] = [MEASUREMENT] 8) [visit] = [VISIT_OCCURRENCE] 9) [death] = [DEATH] 10) [cost] = [COST]\n"+
        "Hint: Desirable sql query will look like: \n" +
        "WITH initial_event AS (SELECT ...your query...), inclusion_criteria AS (SELECT ...your query...), exclusion_criteria AS (SELECT ...your query...) SELECT person_id, gender_concept_id, year_of_birth, month_of_birth, day_of_birth, race_concept_id INTO TEMP TABLE finel_cohort FROM PERSON p JOIN inclusion_criteria ic ON p.person_id = ic.person_id WHERE p.person_id NOT IN (SELECT p.person_id FROM exclusion_criteria);");

        String input = annotation + "\nClinical comments:\n" + explain;
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
        List<ChatCompletionChoice> choices = service.createChatCompletion(chatCompletionRequest).getChoices();
        String sql = choices.get(0).getMessage().getContent();

        return sql;
    }

    public static JSONArray executeSQL(String possql, String dataset) {
        String sql = possql;
        Connection connection = null;
        Statement statement = null;
        JSONArray personArray = new JSONArray();      

        OpenAiService service = new OpenAiService(openaiApiKey, Duration.ofMinutes(20));              
        final List<ChatMessage> messages = new ArrayList<>();

        if (user.equals("Please connect to a database.")){
            JSONObject person = new JSONObject();
            person.accumulate("person_id", "Please connect to a database. Right now, you can only download the JSON file and SQL script.");
            person.accumulate("birth_date", "*");
            person.accumulate("age", "*");
            person.accumulate("gender", "*");
            person.accumulate("race", "*");
            personArray.add(person);
            return personArray;
        }
        // Run the SQL query on database    
        try {
            Class.forName("org.postgresql.Driver");
            if(dataset.equals("SynPUF 1K dataset")){
                connection = DriverManager.getConnection(url1K, user, password);              
                System.out.println("succefully connect to the database: 1K" + "\n");
            } else{
                connection = DriverManager.getConnection(url5pct, user, password);
                System.out.println("succefully connect to the database: 5%" + "\n");
            }
            System.out.println("succefully connect to the database" + connection);
            long startTime = System.currentTimeMillis();
            statement = connection.createStatement();
            
            boolean continueLoop = true;
            int gptTry = 1;
            int sqlExec = 0;
            String error = "";
            while(continueLoop && gptTry < 6 && sqlExec < 6){
                try {
                    System.out.println("\n[[Execute " + gptTry + "th SQL execution]]" + "\n");
                    System.out.println("Print sql: \n" + sql + "\n");
                    statement.executeUpdate(sql);
                    continueLoop = false;
                } catch(SQLException e){
                    System.out.println("\nPrint " + gptTry + "th SQL error: \n" + e.toString() + "\n");
                    error = e.toString();
                    gptTry++;
                }                
                if(error != ""){
                    try {
                        String user2 = "I got the error. Please revise the provided postgreSQL query. Do not omit concept-ids in the query. Return the query surrounding with ```.   Error:" + error.toString();
                        final ChatMessage assistantMessage = new ChatMessage(ChatMessageRole.ASSISTANT.value(), sql);
                        final ChatMessage userMessageRevised = new ChatMessage(ChatMessageRole.USER.value(), user2);
                        messages.add(assistantMessage);
                        messages.add(userMessageRevised);
    
                        ChatCompletionRequest sqlUpdate = ChatCompletionRequest
                                                                            .builder()
                                                                            .model("gpt-4") //gpt-4
                                                                            .messages(messages)
                                                                            .n(1)
                                                                            .temperature(0.0)
                                                                            .build();                       
                        List<ChatCompletionChoice> sql2 = service.createChatCompletion(sqlUpdate).getChoices();
                        sql = sql2.get(0).getMessage().getContent();

                        String pattern = "```([\\s\\S]*?)```";
                        Pattern pat = Pattern.compile(pattern);
                        Matcher mat = pat.matcher(sql);
                        while (mat.find()) {
                            sql = mat.group(1).toString();
                        }
                        sqlExec++;
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
            long endTime = System.currentTimeMillis();            
            System.out.println("\nsql1 execution time:"+(endTime - startTime) + "ms");  
            // formulate the result to send to web server
            startTime = System.currentTimeMillis();
            sql = "select person_id, INITCAP(c1.concept_name) as gender, birth_date, date_part('year',AGE(current_date, birth_date)), c2.concept_name as race\n" +
                    "from (select person_id, gender_concept_id, make_date(year_of_birth,month_of_birth, day_of_birth) as birth_date, race_concept_id\n" +
                    "from person \n" +
                    "where person_id in \n" +
                    "(select person_id from final_cohort))as a\n" +
                    "left join concept as c1 on c1.concept_id = a.gender_concept_id\n" +
                    "left join concept as c2 on c2.concept_id = a.race_concept_id\n" +
                    "order by person_id;";
            ResultSet resultSet = statement.executeQuery(sql);
            endTime = System.currentTimeMillis();
            System.out.println("\nsql2 execution time:"+(endTime - startTime) + "ms");            

            while (resultSet.next()) {
                JSONObject person = new JSONObject();
                String person_id = resultSet.getString(1);
                String gender = resultSet.getString(2);
                String birth_date = resultSet.getString(3);
                String age = resultSet.getString(4).split(" ")[0];
                String race = resultSet.getString(5);
                person.accumulate("person_id", person_id);
                person.accumulate("birth_date", birth_date);
                person.accumulate("age", age);
                person.accumulate("gender", gender);
                person.accumulate("race", race);
                personArray.add(person);
            }
            
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            } finally {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }
        }
        return personArray;
    }

}
