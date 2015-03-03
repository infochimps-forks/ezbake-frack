/*   Copyright (C) 2013-2015 Computer Sciences Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. */

package ezbake.frack.log4j;

import ezbake.frack.context.PipelineContext;
import ezbake.frack.context.PipelineContextProvider;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.helpers.PatternConverter;
import org.apache.log4j.helpers.PatternParser;
import org.apache.log4j.spi.LoggingEvent;

public class FrackPatternLayout extends PatternLayout {
    public static final String FRACK_DEFAULT_PATTERN = "[%f] " + PatternLayout.DEFAULT_CONVERSION_PATTERN;

    public FrackPatternLayout() {
        super(FRACK_DEFAULT_PATTERN);
    }

    public FrackPatternLayout(String pattern) {
        super(pattern);
    }

    @Override
    protected PatternParser createPatternParser(final String pattern) {
        return new PatternParser(pattern) {
            @Override
            protected void finalizeConverter(char c) {
                PatternConverter patternConverter = null;
                if (c == 'f') {
                    patternConverter = new PatternConverter() {
                        @Override
                        protected String convert(LoggingEvent event) {
                            PipelineContext context = PipelineContextProvider.get();
                            if (context != null) {
                                return context.getPipelineId();
                            }
                            return "";
                        }
                    };

                }
                if (patternConverter == null) {
                    super.finalizeConverter(c);
                } else {
                    addConverter(patternConverter);
                }
            }
        };
    }
}
