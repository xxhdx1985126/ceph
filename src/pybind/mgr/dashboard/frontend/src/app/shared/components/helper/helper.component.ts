import { Component, Input } from '@angular/core';
import { Icons } from '../../../shared/enum/icons.enum';

@Component({
  selector: 'cd-helper',
  templateUrl: './helper.component.html',
  styleUrls: ['./helper.component.scss']
})
export class HelperComponent {
  @Input()
  html: any;

  icons = Icons;

  constructor() {}
}
