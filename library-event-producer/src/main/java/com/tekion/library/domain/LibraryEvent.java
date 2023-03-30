package com.tekion.library.domain;


import com.tekion.library.enums.LibraryEventType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.constraints.NotNull;


@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class LibraryEvent {

    private Integer libraryEventId;
    private LibraryEventType libraryEventType;
    @NotNull
    private Book book;
}
