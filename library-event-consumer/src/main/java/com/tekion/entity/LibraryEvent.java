package com.tekion.entity;


import com.tekion.enums.LibraryEventType;
import lombok.*;

import javax.persistence.*;
import javax.validation.constraints.NotNull;


@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
@Entity
public class LibraryEvent {

    @Id
    @GeneratedValue
    private Integer libraryEventId;

    @Enumerated(value = EnumType.STRING)
    private LibraryEventType libraryEventType;
    @NotNull
    @OneToOne(mappedBy = "libraryEvent", cascade = CascadeType.ALL)
    @ToString.Exclude
    private Book book;
}
