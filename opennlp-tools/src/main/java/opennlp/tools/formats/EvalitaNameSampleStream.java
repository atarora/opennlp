/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package opennlp.tools.formats;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import opennlp.tools.commons.Internal;
import opennlp.tools.namefind.NameSample;
import opennlp.tools.util.InputStreamFactory;
import opennlp.tools.util.InvalidFormatException;
import opennlp.tools.util.ObjectStream;
import opennlp.tools.util.PlainTextByLineStream;
import opennlp.tools.util.Span;
import opennlp.tools.util.StringUtil;

/**
 * Parser for the Italian NER training files of the Evalita 2007 and 2009 NER shared tasks.
 * <p>
 * The data does not contain article boundaries,
 * adaptive data will be cleared for every sentence.
 * <p>
 * Named Entities are annotated in the IOB2 format (as used in CoNLL 2002 shared task)
 * <p>
 * The Named Entity tag consists of two parts:
 * 1. The  IOB2 tag: 'B'  (for 'begin')  denotes the  first token  of a
 *    Named Entity,  I (for 'inside')  is used for  all other tokens  in a
 *    Named Entity, and 'O' (for 'outside') is used for all other words;
 * 2. The Entity  type tag: PER  (for Person), ORG  (for Organization),
 *    GPE (for Geo-Political Entity), or LOC (for Location).
 * <p>
 * Each file consists of four columns separated by a blank, containing respectively the token, the
 * <a href="https://www.evalita.it/wp-content/uploads/2021/11/elsnet-tagset-IT.pdf">Elsnet</a>
 * PoS-tag, the Adige news story to which the token belongs, and the Named Entity tag.
 * <p>
 * Data can be found on this
 * <a href="https://www.evalita.it">web site</a>.
 * <p>
 * <b>Note:</b>
 * Do not use this class, internal use only!
 */
@Internal
public class EvalitaNameSampleStream implements ObjectStream<NameSample> {

  public static final String DOCSTART = "-DOCSTART-";
  private static final String CODEC_TAG_O = "O";
  private static final String CODEC_TAG_B = "B-";
  private static final String CODEC_TAG_I = "I-";
  private static final String ENT_TYPE_PER = "PER"; // Person
  private static final String ENT_TYPE_LOC = "LOC"; // Location
  private static final String ENT_TYPE_GPE = "GPE"; // Geo-Political Entity
  private static final String ENT_TYPE_ORG = "ORG"; // Organization

  public enum LANGUAGE {
    IT
  }

  public static final int GENERATE_PERSON_ENTITIES = 0x01;
  public static final int GENERATE_ORGANIZATION_ENTITIES = 0x01 << 1;
  public static final int GENERATE_LOCATION_ENTITIES = 0x01 << 2;
  public static final int GENERATE_GPE_ENTITIES = 0x01 << 3;

  private final LANGUAGE lang;
  private final ObjectStream<String> lineStream;

  private final int types;

  public EvalitaNameSampleStream(LANGUAGE lang, ObjectStream<String> lineStream, int types) {
    this.lang = lang;
    this.lineStream = lineStream;
    this.types = types;
  }

  public EvalitaNameSampleStream(LANGUAGE lang, InputStreamFactory in, int types) throws IOException {
    this(lang, new PlainTextByLineStream(in, StandardCharsets.UTF_8), types);
  }

  private static Span extract(int begin, int end, String beginTag) throws InvalidFormatException {

    String type = beginTag.substring(2);

    type = switch (type) {
      case ENT_TYPE_PER -> "person";
      case ENT_TYPE_LOC -> "location";
      case ENT_TYPE_GPE -> "gpe";
      case ENT_TYPE_ORG -> "organization";
      default -> throw new InvalidFormatException("Unknown type: " + type);
    };

    return new Span(begin, end, type);
  }

  @Override
  public NameSample read() throws IOException {

    List<String> sentence = new ArrayList<>();
    List<String> tags = new ArrayList<>();

    boolean isClearAdaptiveData = false;

    // Empty line indicates end of sentence
    String line;
    while ((line = lineStream.read()) != null && !StringUtil.isEmpty(line)) {

      if (line.startsWith(DOCSTART)) {
        isClearAdaptiveData = true;
        String emptyLine = lineStream.read();

        if (!StringUtil.isEmpty(emptyLine))
          throw new IOException("Empty line after -DOCSTART- not empty: '" + emptyLine + "'!");

        continue;
      }

      String[] fields = line.split(" ");

      // For Italian: WORD  POS-TAG SC-TAG NE-TAG
      if (LANGUAGE.IT.equals(lang) && fields.length == 4) {
        sentence.add(fields[0]);
        tags.add(fields[3]); // 3 is NE-TAG
      }
      else {
        throw new IOException("Incorrect number of fields per line for language: '" + line + "'!");
      }
    }

    // Always clear adaptive data for Italian
    if (LANGUAGE.IT.equals(lang))
      isClearAdaptiveData = true;

    if (!sentence.isEmpty()) {

      // convert name tags into spans
      List<Span> names = new ArrayList<>();

      int beginIndex = -1;
      int endIndex = -1;
      for (int i = 0; i < tags.size(); i++) {

        String tag = tags.get(i);

        if (tag.endsWith(ENT_TYPE_PER) && (types & GENERATE_PERSON_ENTITIES) == 0)
          tag = CODEC_TAG_O;

        if (tag.endsWith(ENT_TYPE_ORG) && (types & GENERATE_ORGANIZATION_ENTITIES) == 0)
          tag = CODEC_TAG_O;

        if (tag.endsWith(ENT_TYPE_LOC) && (types & GENERATE_LOCATION_ENTITIES) == 0)
          tag = CODEC_TAG_O;

        if (tag.endsWith(ENT_TYPE_GPE) && (types & GENERATE_GPE_ENTITIES) == 0)
          tag = CODEC_TAG_O;

        if (tag.startsWith(CODEC_TAG_B)) {

          if (beginIndex != -1) {
            names.add(extract(beginIndex, endIndex, tags.get(beginIndex)));
          }

          beginIndex = i;
          endIndex = i + 1;
        }
        else if (tag.startsWith(CODEC_TAG_I)) {
          endIndex++;
        }
        else if (tag.equals(CODEC_TAG_O)) {
          if (beginIndex != -1) {
            names.add(extract(beginIndex, endIndex, tags.get(beginIndex)));
            beginIndex = -1;
            endIndex = -1;
          }
        }
        else {
          throw new IOException("Invalid tag: " + tag);
        }
      }

      // if one span remains, create it here
      if (beginIndex != -1)
        names.add(extract(beginIndex, endIndex, tags.get(beginIndex)));

      return new NameSample(sentence.toArray(new String[0]),
          names.toArray(new Span[0]), isClearAdaptiveData);
    }
    else if (line != null) {
      // Just filter out empty events, if two lines in a row are empty
      return read();
    }
    else {
      // source stream is not returning anymore lines
      return null;
    }
  }

  @Override
  public void reset() throws IOException, UnsupportedOperationException {
    lineStream.reset();
  }

  @Override
  public void close() throws IOException {
    lineStream.close();
  }
}

