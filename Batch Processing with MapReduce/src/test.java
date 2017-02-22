import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.lang.StringEscapeUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class test {
	public static void main(String[] args) throws IOException{
		BufferedReader br = new BufferedReader(new FileReader("testFile"));
		String line;
		while((line = br.readLine()) != null){
//			System.out.println(StringEscapeUtils.unescapeHtml(line));
			System.out.println(line);
			 DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
	            DocumentBuilder builder = null;
				try {
					builder = factory.newDocumentBuilder();
				} catch (ParserConfigurationException e) {
					e.printStackTrace();
				}
	            Document doc = null;
				try {
					doc = builder.parse(new InputSource(new StringReader(line)));
				} catch (SAXException e) {
					e.printStackTrace();
				}
				NodeList nList = doc.getElementsByTagName("revision");
	            Element element = (Element)nList.item(0);
	            String textContent = element.getElementsByTagName("text").item(0).getTextContent();
		}
		br.close();
	}
}



